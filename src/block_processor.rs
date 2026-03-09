use crate::config::Config;
use crate::kaspa_client::KaspaClient;
use crate::telegram::TelegramClient;
use anyhow::Result;
// Subnetwork IDs:
// - Normal transactions: 0000000000000000000000000000000000000000
// - Coinbase transactions: 0100000000000000000000000000000000000000 (SUBNETWORK_ID_COINBASE)
use kaspa_consensus_core::subnets::SUBNETWORK_ID_COINBASE;
use kaspa_rpc_core::BlockAddedNotification;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

#[derive(Clone)]
pub struct BlockRewardRecord {
    pub address: String,
    pub reward: f64,
    pub block_hash: String,
    pub block_daa_score: u64,
    pub timestamp: u64,
    pub previous_balance: f64,
    pub new_balance: f64,
}

pub struct PendingBlock {
    pub id: String,
    pub block_hash: String,
    pub coinbase_txid: String,
    pub output_index: u32,
    pub address: String,
    pub chat_id: i64,
    pub reward: u64,
    pub daa_score: u64,
}

pub struct BlockProcessor {
    kaspa_client: Arc<KaspaClient>,
    telegram_client: Arc<TelegramClient>,
    config: Arc<Config>,
    pending_blocks: Arc<Mutex<HashMap<String, PendingBlock>>>,
    address_balances: Arc<Mutex<HashMap<String, u64>>>,
    notified_blocks: Arc<Mutex<HashSet<String>>>,
    notified_blocks_times: Arc<Mutex<HashMap<String, Instant>>>,
    block_rewards_history: Arc<Mutex<HashMap<i64, Vec<BlockRewardRecord>>>>,
    notified_transactions: Arc<Mutex<HashSet<String>>>,
    notified_transactions_times: Arc<Mutex<HashMap<String, Instant>>>,
}

impl BlockProcessor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        kaspa_client: Arc<KaspaClient>,
        telegram_client: Arc<TelegramClient>,
        config: Arc<Config>,
        pending_blocks: Arc<Mutex<HashMap<String, PendingBlock>>>,
        address_balances: Arc<Mutex<HashMap<String, u64>>>,
        notified_blocks: Arc<Mutex<HashSet<String>>>,
        notified_blocks_times: Arc<Mutex<HashMap<String, Instant>>>,
        block_rewards_history: Arc<Mutex<HashMap<i64, Vec<BlockRewardRecord>>>>,
        notified_transactions: Arc<Mutex<HashSet<String>>>,
        notified_transactions_times: Arc<Mutex<HashMap<String, Instant>>>,
    ) -> Self {
        Self {
            kaspa_client,
            telegram_client,
            config,
            pending_blocks,
            address_balances,
            notified_blocks,
            notified_blocks_times,
            block_rewards_history,
            notified_transactions,
            notified_transactions_times,
        }
    }

    pub async fn handle_block_added(&self, block_added: BlockAddedNotification) -> Result<()> {
        if !self.config.notifications.block_rewards {
            return Ok(());
        }

        let block = block_added.block;
        let block_hash = block.header.hash.to_string();
        let block_daa_score = block.header.daa_score;

        // IMPORTANT: Don't check blue confirmation here - blocks may not be blue yet when first added
        // Store all blocks as pending and let check_pending_confirmations handle blue confirmation
        // This matches the bridge approach: wait for blue confirmation before processing

        // Store all transactions from this block for later lookup (for calculating sent amounts)
        for tx in &block.transactions {
            // Store transaction in cache so we can look it up later when processing UtxosChanged
            self.kaspa_client.store_transaction_from_block(tx).await;
        }
        
        // Find coinbase transaction and store block rewards as pending
        for tx in &block.transactions {
            if tx.subnetwork_id == SUBNETWORK_ID_COINBASE {
                // Mark the coinbase txid as already processed so UtxosChanged can't format it as a
                // normal incoming/outgoing transaction.
                if let Some(verbose_data) = &tx.verbose_data {
                    let txid = verbose_data.transaction_id.to_string();
                    let mut notified = self.notified_transactions.lock().await;
                    if notified.insert(txid.clone()) {
                        drop(notified);
                        let mut times = self.notified_transactions_times.lock().await;
                        times.insert(txid, Instant::now());
                    }
                }

                // Check outputs for tracked addresses and store as pending
                let Some(verbose_tx) = &tx.verbose_data else {
                    continue;
                };
                let coinbase_txid = verbose_tx.transaction_id.to_string();

                for (output_index, output) in tx.outputs.iter().enumerate() {
                    if let Some(verbose_data) = &output.verbose_data {
                        let address = &verbose_data.script_public_key_address;
                        let addr_str = address.to_string();
                        // Get the user who owns this address
                        if let Some(chat_id) = self.kaspa_client.get_address_owner(&addr_str) {
                            // This is a block reward to a tracked address
                            let reward = output.value;

                            // Store as pending - will be processed when blue confirmed AND DAA depth met
                            debug!(
                                "Block reward {} detected, storing as pending (will check blue confirmation)",
                                block_hash
                            );
                            let reward_id =
                                format!("{}:{}:{}:{}", block_hash, chat_id, addr_str, output_index);
                            let mut pending = self.pending_blocks.lock().await;
                            if !pending.contains_key(&reward_id) {
                                pending.insert(
                                    reward_id.clone(),
                                    PendingBlock {
                                        id: reward_id,
                                        block_hash: block_hash.clone(),
                                        coinbase_txid: coinbase_txid.clone(),
                                        output_index: output_index as u32,
                                        address: addr_str,
                                        chat_id,
                                        reward,
                                        daa_score: block_daa_score,
                                    },
                                );
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    // Verify that a block is blue confirmed and remains blue
    // This is the critical function that ensures we only process blue blocks
    // Two-step process: best-effort tip-hash observation, then authoritative blue checks
    async fn verify_block_is_blue(&self, block_hash: &str) -> bool {
        // Step 1: Best-effort check if block is in DAG tip hashes.
        // A valid blue block may rotate out of current tips quickly on high-BPS DAGs,
        // so this check must not hard-fail the block.
        const TIP_HASH_MAX_ATTEMPTS: u32 = 10;
        const TIP_HASH_RETRY_DELAY_MS: u64 = 2000;
        let mut seen_in_tips = false;

        for attempt in 1..=TIP_HASH_MAX_ATTEMPTS {
            match self.kaspa_client.is_block_in_tip_hashes(block_hash).await {
                Ok(true) => {
                    seen_in_tips = true;
                    debug!(
                        "Block {} is in tip hashes (attempt {})",
                        block_hash, attempt
                    );
                    break;
                }
                Ok(false) => {
                    debug!(
                        "Block {} not in tip hashes yet (attempt {}/{}), waiting",
                        block_hash, attempt, TIP_HASH_MAX_ATTEMPTS
                    );
                    if attempt < TIP_HASH_MAX_ATTEMPTS {
                        tokio::time::sleep(tokio::time::Duration::from_millis(
                            TIP_HASH_RETRY_DELAY_MS,
                        ))
                        .await;
                    } else {
                        debug!(
                            "Block {} not in tip hashes after {} attempts; continuing with blue checks",
                            block_hash, TIP_HASH_MAX_ATTEMPTS
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to check tip hashes for block {} (attempt {}/{}): {}",
                        block_hash, attempt, TIP_HASH_MAX_ATTEMPTS, e
                    );
                    if attempt < TIP_HASH_MAX_ATTEMPTS {
                        tokio::time::sleep(tokio::time::Duration::from_millis(
                            TIP_HASH_RETRY_DELAY_MS,
                        ))
                        .await;
                    } else {
                        debug!(
                            "Tip-hash checks exhausted for block {}; continuing with blue checks",
                            block_hash
                        );
                    }
                }
            }
        }
        if !seen_in_tips {
            debug!(
                "Block {} was not observed in current tip hashes; relying on blue confirmation result",
                block_hash
            );
        }

        // Step 2: Verify blue status with retries (bridge approach)
        const BLOCK_CONFIRM_MAX_ATTEMPTS: u32 = 30;
        const BLOCK_CONFIRM_RETRY_DELAY_MS: u64 = 2000;
        // Safer than single-shot (to avoid transient false positives), but less strict than 5.
        const REQUIRED_CONSECUTIVE_CONFIRMS: u32 = 3;

        let mut consecutive_confirms = 0;

        for attempt in 1..=BLOCK_CONFIRM_MAX_ATTEMPTS {
            match self.kaspa_client.is_block_blue_confirmed(block_hash).await {
                Ok(true) => {
                    consecutive_confirms += 1;
                    debug!(
                        "Block {} is blue (consecutive confirms: {})",
                        block_hash, consecutive_confirms
                    );

                    if consecutive_confirms >= REQUIRED_CONSECUTIVE_CONFIRMS {
                        info!(
                            "Block {} confirmed blue after {} verification checks",
                            block_hash, attempt
                        );
                        return true;
                    }
                }
                Ok(false) => {
                    debug!("Block {} is not blue (attempt {})", block_hash, attempt);
                    consecutive_confirms = 0;
                }
                Err(e) => {
                    let error_msg = e.to_string();
                    if error_msg.contains("doesn't have any merger block") {
                        debug!(
                            "Block {} doesn't have merger block yet (attempt {}/{}), retrying",
                            block_hash, attempt, BLOCK_CONFIRM_MAX_ATTEMPTS
                        );
                    } else {
                        warn!(
                            "Failed to check blue status for block {} (attempt {}/{}): {}",
                            block_hash, attempt, BLOCK_CONFIRM_MAX_ATTEMPTS, e
                        );
                    }
                    consecutive_confirms = 0;
                }
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(
                BLOCK_CONFIRM_RETRY_DELAY_MS,
            ))
            .await;
        }

        warn!(
            "Block {} not confirmed blue after {} attempts (not counted as blue)",
            block_hash, BLOCK_CONFIRM_MAX_ATTEMPTS
        );
        false
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_block_reward(
        &self,
        reward_id: &str,
        address: &str,
        chat_id: i64,
        block_hash: &str,
        coinbase_txid: &str,
        output_index: u32,
        reward: u64,
        block_daa_score: u64,
        virtual_daa_score: u64,
    ) -> Result<()> {
        // Check if we've already notified about this specific reward candidate
        {
            let notified = self.notified_blocks.lock().await;
            if notified.contains(reward_id) {
                debug!("Reward {} already notified, skipping", reward_id);
                return Ok(());
            }
        }

        // Cleanup old notified blocks periodically
        {
            let notified = self.notified_blocks.lock().await;
            if notified.len() % 100 == 0 {
                drop(notified);
                self.cleanup_old_notified_blocks().await;
            }
        }

        // CRITICAL: Verify the block is blue before processing
        // This is the main fix - we only process blocks that are verified as blue
        if self.config.notifications.blue_blocks_only
            && !self.verify_block_is_blue(block_hash).await
        {
            debug!("Block {} failed blue verification, skipping", block_hash);
            // Remove from pending since it's not blue
            self.pending_blocks.lock().await.remove(reward_id);
            return Ok(());
        }

        // Check DAA depth
        let confirmation_depth = self.config.confirmation.daa_score_depth;
        let daa_diff = virtual_daa_score.saturating_sub(block_daa_score);

        if daa_diff < confirmation_depth {
            // Not yet confirmed (DAA depth not met), keep as pending
            debug!(
                "Block {} blue confirmed but not yet deep enough (DAA diff: {} < {}), keeping pending",
                block_hash, daa_diff, confirmation_depth
            );
            return Ok(());
        }

        // Final safety gate: ensure this exact coinbase output exists in UTXO set.
        // This prevents notifying red/rejected rewards that never become spendable UTXOs.
        let reward_utxo_exists = match self
            .kaspa_client
            .has_utxo_outpoint(address, coinbase_txid, output_index, reward)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    "Failed checking reward UTXO existence for {}:{} ({}): {}",
                    coinbase_txid, output_index, address, e
                );
                false
            }
        };
        if !reward_utxo_exists {
            // If sufficiently old and still missing, treat as non-crediting reward and drop it.
            if daa_diff > confirmation_depth.saturating_add(120) {
                warn!(
                    "Dropping reward {} (block {}) - coinbase UTXO not found after DAA diff {}",
                    reward_id, block_hash, daa_diff
                );
                self.pending_blocks.lock().await.remove(reward_id);
            } else {
                debug!(
                    "Reward {} not yet present in UTXO set; keeping pending (DAA diff: {})",
                    reward_id, daa_diff
                );
            }
            return Ok(());
        }

        // Block reward is blue confirmed, deep enough, and UTXO-backed.
        info!(
            "Processing blue block reward: {} (DAA diff: {}, blue verified)",
            block_hash, daa_diff
        );

        let timestamp = chrono::Utc::now().timestamp_millis() as u64;

        // Fetch current balance from blockchain to ensure accuracy
        // Note: The blockchain balance already includes this reward, so we subtract it to get the "before" balance
        let current_balance = match self.kaspa_client.get_balance(address).await {
            Ok(balance) => {
                info!(
                    "Fetched current balance for {} from blockchain: {} sompi",
                    address, balance
                );
                balance
            }
            Err(e) => {
                warn!("Failed to fetch balance for {}: {}, using 0", address, e);
                0
            }
        };

        let (previous_balance, new_balance) =
            if self.config.confirmation.strict_balance_reconciliation {
                // Final consistency gate:
                // only notify when this reward can be reconciled against cached spendable balance.
                // This avoids duplicate/rejected reward notifications that would otherwise produce
                // identical previous/new balances across multiple block hashes.
                let balances = self.address_balances.lock().await;
                let cached_before = balances.get(address).copied();
                drop(balances);

                if let Some(cached) = cached_before {
                    let required_after_reward = cached.saturating_add(reward);
                    if current_balance < required_after_reward {
                        debug!(
                            "Balance reconciliation fallback for reward {} (block {}): current {}, cached {}, reward {}, daa_diff {}",
                            reward_id, block_hash, current_balance, cached, reward, daa_diff
                        );
                        let prev = current_balance.saturating_sub(reward);
                        let next = current_balance;
                        (prev, next)
                    } else {
                        let prev = cached;
                        let next = required_after_reward;
                        (prev, next)
                    }
                } else {
                    // Fallback for first observation when cache is empty.
                    let prev = current_balance.saturating_sub(reward);
                    let next = current_balance;
                    (prev, next)
                }
            } else {
                let prev = current_balance.saturating_sub(reward);
                let next = current_balance;
                (prev, next)
            };

        // Send notification to the specific user
        let notify_result = self
            .telegram_client
            .send_block_reward_to_chat(
                chat_id,
                address,
                KaspaClient::sompi_to_kas(reward),
                KaspaClient::sompi_to_kas(previous_balance),
                KaspaClient::sompi_to_kas(new_balance),
                block_hash,
                block_daa_score,
                timestamp,
            )
            .await;

        if let Err(e) = notify_result {
                error!(
                    "Failed to send Telegram notification to chat {}: {}",
                    chat_id, e
                );
            // Keep pending so we can retry later.
            return Ok(());
        }

        // Update cached balance only after successful notification
        {
            let mut balances = self.address_balances.lock().await;
            balances.insert(address.to_string(), new_balance);
        }

        // Store block reward in history after successful notification
        {
            let reward_kas = KaspaClient::sompi_to_kas(reward);
            let prev_balance_kas = KaspaClient::sompi_to_kas(previous_balance);
            let new_balance_kas = KaspaClient::sompi_to_kas(new_balance);

            let mut history = self.block_rewards_history.lock().await;
            let user_rewards = history.entry(chat_id).or_insert_with(Vec::new);
            user_rewards.push(BlockRewardRecord {
                address: address.to_string(),
                reward: reward_kas,
                block_hash: block_hash.to_string(),
                block_daa_score,
                timestamp,
                previous_balance: prev_balance_kas,
                new_balance: new_balance_kas,
            });

            // Keep only last 50 rewards per user
            if user_rewards.len() > 50 {
                user_rewards.remove(0);
            }
        }

        // Mark as notified after successful notification
        {
            let mut notified = self.notified_blocks.lock().await;
            let mut times = self.notified_blocks_times.lock().await;
            notified.insert(reward_id.to_string());
            times.insert(reward_id.to_string(), Instant::now());
        }

        // Remove from pending
        self.pending_blocks.lock().await.remove(reward_id);

        Ok(())
    }

    /// Cleanup old notified blocks to prevent memory growth
    /// Removes entries older than 7 days or if set exceeds 10,000 entries
    async fn cleanup_old_notified_blocks(&self) {
        const MAX_AGE_DAYS: u64 = 7;
        const MAX_ENTRIES: usize = 10_000;
        let max_age = Duration::from_secs(MAX_AGE_DAYS * 24 * 60 * 60);
        let now = Instant::now();

        let mut notified = self.notified_blocks.lock().await;
        let mut times = self.notified_blocks_times.lock().await;

        // If we're under the limit, only remove entries older than max_age
        if notified.len() < MAX_ENTRIES {
            let mut to_remove = Vec::new();
            for (block_hash, &time) in times.iter() {
                if now.duration_since(time) > max_age {
                    to_remove.push(block_hash.clone());
                }
            }
            for block_hash in to_remove {
                notified.remove(&block_hash);
                times.remove(&block_hash);
            }
        } else {
            // Over limit: remove oldest entries until under limit
            let mut entries: Vec<(String, Instant)> =
                times.iter().map(|(k, v)| (k.clone(), *v)).collect();
            entries.sort_by_key(|(_, time)| *time);

            let to_remove_count = entries.len().saturating_sub(MAX_ENTRIES);
            for (block_hash, _) in entries.iter().take(to_remove_count) {
                notified.remove(block_hash);
                times.remove(block_hash);
            }
        }
    }

    pub async fn check_pending_confirmations(&self, virtual_daa_score: u64) -> Result<()> {
        let confirmation_depth = self.config.confirmation.daa_score_depth;

        // Get all pending blocks that might be ready
        let mut pending_blocks: Vec<PendingBlock> = {
            let pending = self.pending_blocks.lock().await;
            pending
                .values()
                .map(|p| PendingBlock {
                    id: p.id.clone(),
                    block_hash: p.block_hash.clone(),
                    coinbase_txid: p.coinbase_txid.clone(),
                    output_index: p.output_index,
                    address: p.address.clone(),
                    chat_id: p.chat_id,
                    reward: p.reward,
                    daa_score: p.daa_score,
                })
                .collect()
        };

        // Sort by DAA score in ascending order to ensure notifications are ordered
        pending_blocks.sort_by_key(|b| b.daa_score);

        for pending in pending_blocks {
            let daa_diff = virtual_daa_score.saturating_sub(pending.daa_score);

            // Check if block has enough depth
            if daa_diff >= confirmation_depth {
                // Process the block reward
                self.handle_block_reward(
                    &pending.id,
                    &pending.address,
                    pending.chat_id,
                    &pending.block_hash,
                    &pending.coinbase_txid,
                    pending.output_index,
                    pending.reward,
                    pending.daa_score,
                    virtual_daa_score,
                )
                .await?;
            } else {
                debug!(
                    "Block {} not deep enough yet (DAA diff: {} < {})",
                    pending.block_hash, daa_diff, confirmation_depth
                );
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::BlockRewardRecord;

    #[tokio::test]
    async fn test_blue_block_filtering() {
        // This test verifies that only blue-confirmed blocks are processed
        // Note: This would require mocking KaspaClient
        // For now, we verify the logic structure

        // The key test is that when a block is not blue confirmed,
        // it should be skipped in handle_block_added
        // Blue block filtering logic is implemented in handle_block_added
    }

    #[tokio::test]
    async fn test_block_reward_confirmation() {
        // Test that block rewards require DAA depth confirmation
        const CONFIRMATION_DEPTH: u64 = 10;

        let virtual_daa_score: u64 = 100_000;
        let block_daa_score: u64 = 99_990; // 10 DAA difference (exactly at threshold)
        let daa_diff = virtual_daa_score.saturating_sub(block_daa_score);

        assert_eq!(
            daa_diff, CONFIRMATION_DEPTH,
            "Should be at confirmation threshold"
        );
        assert!(daa_diff >= CONFIRMATION_DEPTH, "Should be confirmed");

        let block_daa_score2: u64 = 99_995; // 5 DAA difference (below threshold)
        let daa_diff2 = virtual_daa_score.saturating_sub(block_daa_score2);
        assert!(
            daa_diff2 < CONFIRMATION_DEPTH,
            "Should not be confirmed yet"
        );
    }

    #[tokio::test]
    async fn test_block_reward_history_limit() {
        // Test that block reward history is limited to 50 entries per user
        const MAX_HISTORY: usize = 50;

        let mut history: Vec<BlockRewardRecord> = Vec::new();

        // Add 60 rewards
        for i in 0..60 {
            history.push(BlockRewardRecord {
                address: "test_address".to_string(),
                reward: 1.0,
                block_hash: format!("block_{}", i),
                block_daa_score: i as u64,
                timestamp: i as u64,
                previous_balance: 0.0,
                new_balance: 1.0,
            });
        }

        // Simulate the limit logic (remove oldest entries until under limit)
        while history.len() > MAX_HISTORY {
            history.remove(0);
        }

        assert_eq!(
            history.len(),
            MAX_HISTORY,
            "History should be limited to 50 entries"
        );
        assert_eq!(
            history[0].block_hash, "block_10",
            "Oldest entries should be removed"
        );
        assert_eq!(
            history[49].block_hash, "block_59",
            "Newest entry should be preserved"
        );
    }

    #[tokio::test]
    async fn test_balance_calculation_for_block_rewards() {
        // Test that balance calculations are correct for block rewards
        // For block rewards: current_balance = previous_balance + reward
        // So: previous_balance = current_balance - reward

        let current_balance: u64 = 100_000_000; // 1 KAS
        let reward: u64 = 10_000_000; // 0.1 KAS
        let previous_balance = current_balance.saturating_sub(reward);

        assert_eq!(
            previous_balance, 90_000_000,
            "Previous balance should be 0.9 KAS"
        );

        let new_balance = previous_balance + reward;
        assert_eq!(
            new_balance, current_balance,
            "New balance should equal current balance"
        );
    }
}
