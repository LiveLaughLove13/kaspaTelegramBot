use crate::config::Config;
use crate::kaspa_client::KaspaClient;
use crate::telegram::TelegramClient;
use crate::transaction_processor::PendingTransaction;
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

fn should_store_incoming_pending(is_in_block: bool, daa_diff: u64, confirmation_depth: u64) -> bool {
    !is_in_block && daa_diff < confirmation_depth
}

fn calculate_incoming_balances(current_balance: u64, received_amount: u64) -> (u64, u64) {
    let previous_balance = current_balance.saturating_sub(received_amount);
    (previous_balance, current_balance)
}

pub struct IncomingTransactionHandler {
    kaspa_client: Arc<KaspaClient>,
    telegram_client: Arc<TelegramClient>,
    config: Arc<Config>,
    pending_transactions: Arc<Mutex<HashMap<String, PendingTransaction>>>,
    address_balances: Arc<Mutex<HashMap<String, u64>>>,
    notified_transactions: Arc<Mutex<HashSet<String>>>,
    notified_transactions_times: Arc<Mutex<HashMap<String, Instant>>>,
}

impl IncomingTransactionHandler {
    pub fn new(
        kaspa_client: Arc<KaspaClient>,
        telegram_client: Arc<TelegramClient>,
        config: Arc<Config>,
        pending_transactions: Arc<Mutex<HashMap<String, PendingTransaction>>>,
        address_balances: Arc<Mutex<HashMap<String, u64>>>,
        notified_transactions: Arc<Mutex<HashSet<String>>>,
        notified_transactions_times: Arc<Mutex<HashMap<String, Instant>>>,
    ) -> Self {
        Self {
            kaspa_client,
            telegram_client,
            config,
            pending_transactions,
            address_balances,
            notified_transactions,
            notified_transactions_times,
        }
    }

    pub async fn handle_incoming_tx(
        &self,
        address: &str,
        chat_id: i64,
        txid: String,
        amount: u64,
        tx_daa_score: u64,
        virtual_daa_score: u64,
    ) -> Result<()> {
        // Check if we've already notified about this transaction (early exit to avoid duplicate work)
        {
            let notified = self.notified_transactions.lock().await;
            if notified.contains(&txid) {
                debug!("Transaction {} already notified, skipping duplicate", txid);
                return Ok(());
            }
        }

        // Cleanup old notified transactions periodically (keep last 7 days worth, ~10,000 entries)
        // Only cleanup every 100th transaction to avoid performance impact
        {
            let notified = self.notified_transactions.lock().await;
            if notified.len() % 100 == 0 {
                drop(notified);
                self.cleanup_old_notified_transactions().await;
            }
        }

        // Check if transaction is already in a block (not in mempool)
        // If it's not in mempool, it's already confirmed and we can process it immediately
        let mempool_entry_opt = self
            .kaspa_client
            .get_mempool_entry(&txid)
            .await
            .ok()
            .flatten();
        let is_in_block = mempool_entry_opt.is_none();

        let confirmation_depth = self.config.confirmation.daa_score_depth;
        let daa_diff = virtual_daa_score.saturating_sub(tx_daa_score);

        // If transaction is already in a block (confirmed), process immediately
        // Otherwise, wait for DAA depth confirmation
        if should_store_incoming_pending(is_in_block, daa_diff, confirmation_depth) {
            // Not yet confirmed, store as pending
            debug!(
                "Transaction {} not yet confirmed (DAA diff: {} < {}, in_mempool: true)",
                txid, daa_diff, confirmation_depth
            );
            let mut pending = self.pending_transactions.lock().await;
            // Only add to pending if not already there (avoid duplicate pending entries)
            if !pending.contains_key(&txid) {
                pending.insert(
                    txid.clone(),
                    PendingTransaction {
                        txid: txid.clone(),
                        address: address.to_string(),
                        chat_id,
                        amount: amount as i64,
                        net_change: None, // Not used for incoming transactions
                        daa_score: tx_daa_score,
                        is_incoming: true,
                    },
                );
            }
            return Ok(());
        }

        // Transaction is confirmed and not yet notified
        info!(
            "Incoming transaction confirmed: {} (DAA diff: {}, in_block: {})",
            txid, daa_diff, is_in_block
        );

        // Check if already notified (early exit to prevent duplicate work)
        // IMPORTANT: We check but DON'T mark yet - we'll mark AFTER successfully sending
        // This allows retries if notification fails
        {
            let notified = self.notified_transactions.lock().await;
            if notified.contains(&txid) {
                info!(
                    "Transaction {} already marked as notified, skipping duplicate notification attempt",
                    txid
                );
                return Ok(());
            }
        }

        // Get actual received amount from transaction outputs (more accurate than UTXO calculation)
        // For incoming transactions, we want the sum of outputs TO the tracked address
        let actual_received_amount = match self.kaspa_client.get_transaction_sent_amount(&txid, address).await {
            Ok(Some((_sent_to_others, received_to_tracked))) => {
                // received_to_tracked is the amount that came to the tracked address
                if received_to_tracked > 0 {
                    info!(
                        "Incoming transaction {}: Calculated from transaction outputs - received: {} sompi ({} KAS) (outputs to tracked address)",
                        txid,
                        received_to_tracked,
                        KaspaClient::sompi_to_kas(received_to_tracked)
                    );
                    received_to_tracked
                } else {
                    // Fallback to amount from UTXO calculation
                    amount
                }
            }
            Ok(None) => {
                // Couldn't get transaction details, use amount from UTXO calculation
                warn!(
                    "Incoming transaction {}: Could not get transaction details from mempool/cache. Using UTXO amount: {} sompi ({} KAS)",
                    txid,
                    amount,
                    KaspaClient::sompi_to_kas(amount)
                );
                amount
            }
            Err(e) => {
                warn!(
                    "Incoming transaction {}: Failed to get transaction details: {}. Using UTXO amount: {} sompi ({} KAS)",
                    txid, e, amount, KaspaClient::sompi_to_kas(amount)
                );
                amount
            }
        };

        // Get transaction timestamp from mempool entry or use current time
        let timestamp = match mempool_entry_opt {
            Some(entry) => entry
                .transaction
                .verbose_data
                .map(|vd| vd.block_time)
                .unwrap_or_else(|| chrono::Utc::now().timestamp_millis() as u64),
            None => {
                // Transaction not in mempool (already confirmed), use current time
                chrono::Utc::now().timestamp_millis() as u64
            }
        };

        // CRITICAL: Always fetch balance from blockchain to ensure accuracy
        // The blockchain balance already includes this transaction, so we calculate previous balance
        // For incoming transactions: current_balance = previous_balance + amount
        // So: previous_balance = current_balance - amount
        let current_balance = match self.kaspa_client.get_balance(address).await {
            Ok(balance) => {
                info!(
                    "Fetched current balance for {} from blockchain: {} sompi (includes this transaction)",
                    address, balance
                );
                balance
            }
            Err(e) => {
                warn!(
                    "Failed to fetch balance for {} from blockchain: {}, using cached or 0",
                    address, e
                );
                // Fallback to cached balance if available, otherwise 0
                let balances = self.address_balances.lock().await;
                balances.get(address).copied().unwrap_or(0)
            }
        };

        // Calculate previous balance: current balance already includes this incoming transaction
        // Use actual_received_amount instead of amount for more accuracy
        let (previous_balance, new_balance) =
            calculate_incoming_balances(current_balance, actual_received_amount);

        // Update cached balance with the fresh blockchain balance
        // This ensures the cache is always in sync with the blockchain
        {
            let mut balances = self.address_balances.lock().await;
            let old_cached = balances.get(address).copied();
            balances.insert(address.to_string(), new_balance);
            if let Some(old) = old_cached {
                if old != new_balance {
                    info!(
                        "Updated cached balance for {}: {} -> {} sompi (change: {} sompi)",
                        address,
                        old,
                        new_balance,
                        new_balance.saturating_sub(old)
                    );
                }
            }
        }

        info!(
            "Incoming transaction balance: previous = {} sompi, amount = {} sompi (actual received: {} sompi), new = {} sompi",
            previous_balance, amount, actual_received_amount, new_balance
        );

        // Send notification to the specific user
        // Use actual_received_amount for display (more accurate than UTXO calculation)
        match self
            .telegram_client
            .send_incoming_tx_to_chat(
                chat_id,
                address,
                KaspaClient::sompi_to_kas(actual_received_amount),
                KaspaClient::sompi_to_kas(previous_balance),
                KaspaClient::sompi_to_kas(new_balance),
                &txid,
                tx_daa_score,
                timestamp,
            )
            .await
        {
            Ok(_) => {
                info!(
                    "✅ Successfully sent incoming transaction notification to chat {} for txid {}",
                    chat_id, txid
                );
                // CRITICAL: Only mark as notified AFTER successfully sending
                // This allows retries if notification fails
                {
                    let mut notified = self.notified_transactions.lock().await;
                    notified.insert(txid.clone());
                }
                {
                    let mut times = self.notified_transactions_times.lock().await;
                    times.insert(txid.clone(), Instant::now());
                }
            }
            Err(e) => {
                error!(
                    "❌ FAILED to send Telegram notification to chat {} for txid {}: {}",
                    chat_id, txid, e
                );
                // IMPORTANT: Don't mark as notified if notification failed
                // This allows the transaction to be retried on the next UtxosChanged notification
                // Return error so the caller knows it failed
                return Err(anyhow::anyhow!(
                    "Failed to send incoming transaction notification: {}",
                    e
                ));
            }
        }

        // Remove from pending
        self.pending_transactions.lock().await.remove(&txid);

        Ok(())
    }

    // Cleanup old notified transactions to prevent memory growth
    // Removes entries older than 7 days or if set exceeds 10,000 entries
    async fn cleanup_old_notified_transactions(&self) {
        use std::time::Duration;
        const MAX_AGE_DAYS: u64 = 7;
        const MAX_ENTRIES: usize = 10_000;
        let max_age = Duration::from_secs(MAX_AGE_DAYS * 24 * 60 * 60);
        let now = Instant::now();

        let mut notified = self.notified_transactions.lock().await;
        let mut times = self.notified_transactions_times.lock().await;

        // If we're under the limit, only remove entries older than max_age
        if notified.len() < MAX_ENTRIES {
            let mut to_remove = Vec::new();
            for (txid, &time) in times.iter() {
                if now.duration_since(time) > max_age {
                    to_remove.push(txid.clone());
                }
            }
            for txid in to_remove {
                notified.remove(&txid);
                times.remove(&txid);
            }
        } else {
            // Over limit: remove oldest entries until under limit
            let mut entries: Vec<(String, Instant)> =
                times.iter().map(|(k, v)| (k.clone(), *v)).collect();
            entries.sort_by_key(|(_, time)| *time);

            let to_remove_count = entries.len().saturating_sub(MAX_ENTRIES);
            for (txid, _) in entries.iter().take(to_remove_count) {
                notified.remove(txid);
                times.remove(txid);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{calculate_incoming_balances, should_store_incoming_pending};

    #[test]
    fn incoming_pending_gate_respects_block_and_depth() {
        assert!(should_store_incoming_pending(false, 2, 10));
        assert!(!should_store_incoming_pending(true, 0, 10));
        assert!(!should_store_incoming_pending(false, 10, 10));
        assert!(!should_store_incoming_pending(false, 11, 10));
    }

    #[test]
    fn incoming_balance_math_is_consistent() {
        let (prev, new) = calculate_incoming_balances(1_000, 250);
        assert_eq!(prev, 750);
        assert_eq!(new, 1_000);
    }

    #[test]
    fn incoming_balance_math_saturates_at_zero() {
        let (prev, new) = calculate_incoming_balances(100, 500);
        assert_eq!(prev, 0);
        assert_eq!(new, 100);
    }
}
