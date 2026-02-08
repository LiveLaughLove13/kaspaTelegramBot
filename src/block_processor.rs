use crate::config::Config;
use crate::kaspa_client::KaspaClient;
use crate::telegram::TelegramClient;
use anyhow::Result;
// Subnetwork IDs:
// - Normal transactions: 0000000000000000000000000000000000000000
// - Coinbase transactions: 0100000000000000000000000000000000000000000000 (SUBNETWORK_ID_COINBASE)
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

#[derive(Clone)]
pub struct PendingBlock {
    pub block_hash: String,
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

        // IMPORTANT: We DON'T check if the block is blue here because it's too early
        // Instead, we store all blocks as pending and verify they become blue later
        // This is the key fix - we were filtering too early

        // First, find and mark coinbase transactions to prevent double processing
        for tx in &block.transactions {
            if tx.subnetwork_id == SUBNETWORK_ID_COINBASE {
                // Mark the coinbase txid as already processed
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
                // We'll verify blue status later in the confirmation process
                for output in &tx.outputs {
                    if let Some(verbose_data) = &output.verbose_data {
                        let address = &verbose_data.script_public_key_address;
                        let addr_str = address.to_string();

                        // Get the user who owns this address
                        if let Some(chat_id) = self.kaspa_client.get_address_owner(&addr_str) {
                            // This is a block reward to a tracked address
                            let reward = output.value;

                            debug!(
                                "Found potential block reward {} for address {}, storing as pending for blue verification",
                                block_hash, addr_str
                            );

                            // Store as pending - will be verified for blue status later
                            let mut pending = self.pending_blocks.lock().await;
                            if !pending.contains_key(&block_hash) {
                                pending.insert(
                                    block_hash.clone(),
                                    PendingBlock {
                                        block_hash: block_hash.clone(),
                                        address: addr_str.clone(),
                                        chat_id,
                                        reward,
                                        daa_score: block_daa_score,
                                    },
                                );
                                debug!(
                                    "Stored block {} as pending for address {}, will verify blue status",
                                    block_hash, addr_str
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
    // Two-step process: check tip hashes first, then verify blue status
async fn verify_block_is_blue(&self, block_hash: &str) -> bool {
    // Step 1: Check if block is in DAG tip hashes (bridge approach)
    const TIP_HASH_MAX_ATTEMPTS: u32 = 10;
    const TIP_HASH_RETRY_DELAY_MS: u64 = 2000;

    for attempt in 1..=TIP_HASH_MAX_ATTEMPTS {
        match self.kaspa_client.is_block_in_tip_hashes(block_hash).await {
            Ok(true) => {
                debug!("Block {} is in tip hashes (attempt {})", block_hash, attempt);
                break;
            }
            Ok(false) => {
                debug!("Block {} not in tip hashes yet (attempt {}/{}), waiting",
                        block_hash, attempt, TIP_HASH_MAX_ATTEMPTS);
                if attempt < TIP_HASH_MAX_ATTEMPTS {
                    tokio::time::sleep(tokio::time::Duration::from_millis(TIP_HASH_RETRY_DELAY_MS)).await;
                } else {
                    warn!("Block {} not in tip hashes after {} attempts, skipping",
                           block_hash, TIP_HASH_MAX_ATTEMPTS);
                    return false;
                }
            }
            Err(e) => {
                warn!("Failed to check tip hashes for block {} (attempt {}/{}): {}",
                       block_hash, attempt, TIP_HASH_MAX_ATTEMPTS, e);
                if attempt < TIP_HASH_MAX_ATTEMPTS {
                    tokio::time::sleep(tokio::time::Duration::from_millis(TIP_HASH_RETRY_DELAY_MS)).await;
                } else {
                    return false;
                }
            }
        }
    }

    // Step 2: Verify blue status with retries (bridge approach)
    const BLOCK_CONFIRM_MAX_ATTEMPTS: u32 = 30;
    const BLOCK_CONFIRM_RETRY_DELAY_MS: u64 = 2000;
    const REQUIRED_CONSECUTIVE_CONFIRMS: u32 = 5;

    let mut consecutive_confirms = 0;

    for attempt in 1..=BLOCK_CONFIRM_MAX_ATTEMPTS {
        match self.kaspa_client.is_block_blue_confirmed(block_hash).await {
            Ok(true) => {
                consecutive_confirms += 1;
                debug!("Block {} is blue (consecutive confirms: {})",
                        block_hash, consecutive_confirms);

                if consecutive_confirms >= REQUIRED_CONSECUTIVE_CONFIRMS {
                    info!("Block {} confirmed blue after {} verification checks",
                          block_hash, attempt);
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
                    debug!("Block {} doesn't have merger block yet (attempt {}/{}), retrying",
                            block_hash, attempt, BLOCK_CONFIRM_MAX_ATTEMPTS);
                } else {
                    warn!("Failed to check blue status for block {} (attempt {}/{}): {}",
                           block_hash, attempt, BLOCK_CONFIRM_MAX_ATTEMPTS, e);
                }
                consecutive_confirms = 0;
            }
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(BLOCK_CONFIRM_RETRY_DELAY_MS)).await;
    }

    warn!("Block {} not confirmed blue after {} attempts (not counted as blue)",
          block_hash, BLOCK_CONFIRM_MAX_ATTEMPTS);
    false
}

    async fn handle_block_reward(
        &self,
        address: &str,
        chat_id: i64,
        block_hash: &str,
        reward: u64,
        block_daa_score: u64,
        virtual_daa_score: u64,
    ) -> Result<()> {
        // Check if we've already notified about this block
        {
            let notified = self.notified_blocks.lock().await;
            if notified.contains(block_hash) {
                debug!("Block {} already notified, skipping", block_hash);
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
            self.pending_blocks.lock().await.remove(block_hash);
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

        // Block reward is blue confirmed AND DAA depth met - safe to process
        info!(
            "Processing blue block reward: {} (DAA diff: {}, blue verified)",
            block_hash, daa_diff
        );

        // Mark as notified
        {
            let mut notified = self.notified_blocks.lock().await;
            let mut times = self.notified_blocks_times.lock().await;
            notified.insert(block_hash.to_string());
            times.insert(block_hash.to_string(), Instant::now());
        }

        let timestamp = chrono::Utc::now().timestamp_millis() as u64;

        // Fetch current balance from blockchain to ensure accuracy
        // Note: The blockchain balance already includes this reward, so we subtract it to get the "before" balance
        let current_balance = match self.kaspa_client.get_balance(address).await {
            Ok(balance) => {
                info!("Fetched current balance for {} from blockchain: {} sompi", address, balance);
                balance
            }
            Err(e) => {
                warn!("Failed to fetch balance for {}: {}, using 0", address, e);
                0
            }
        };

        // The current balance already includes this reward, so subtract it to get the "before" balance
        let previous_balance = current_balance.saturating_sub(reward);
        let new_balance = current_balance;
        let mut balances = self.address_balances.lock().await;
        balances.insert(address.to_string(), new_balance);

        // Store block reward in history
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

        // Send notification
        self.telegram_client
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
            .await
            .unwrap_or_else(|e| {
                error!(
                    "Failed to send Telegram notification to chat {}: {}",
                    chat_id, e
                )
            });

        // Remove from pending
        self.pending_blocks.lock().await.remove(block_hash);

        Ok(())
    }

    // Cleanup old notified blocks
    async fn cleanup_old_notified_blocks(&self) {
        const MAX_AGE_DAYS: u64 = 7;
        const MAX_ENTRIES: usize = 10_000;
        let max_age = Duration::from_secs(MAX_AGE_DAYS * 24 * 60 * 60);
        let now = Instant::now();

        let mut notified = self.notified_blocks.lock().await;
        let mut times = self.notified_blocks_times.lock().await;

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
            pending.values().cloned().collect()
        };

        // Sort by DAA score in ascending order to ensure notifications are ordered
        pending_blocks.sort_by_key(|b| b.daa_score);

        for pending in pending_blocks {
            let daa_diff = virtual_daa_score.saturating_sub(pending.daa_score);

            // Check if block has enough depth
            if daa_diff >= confirmation_depth {
                // Process the block reward
                self.handle_block_reward(
                    &pending.address,
                    pending.chat_id,
                    &pending.block_hash,
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
