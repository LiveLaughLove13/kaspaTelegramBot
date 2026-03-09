use crate::config::Config;
use crate::kaspa_client::KaspaClient;
use crate::telegram::TelegramClient;
use crate::transaction_processor::PendingTransaction;
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

fn should_store_outgoing_pending(is_in_block: bool, daa_diff: u64, confirmation_depth: u64) -> bool {
    !is_in_block && daa_diff < confirmation_depth
}

fn calculate_outgoing_balances(current_balance: u64, net_change: u64) -> (u64, u64) {
    let previous_balance = current_balance.saturating_add(net_change);
    (previous_balance, current_balance)
}

pub struct OutgoingTransactionHandler {
    kaspa_client: Arc<KaspaClient>,
    telegram_client: Arc<TelegramClient>,
    config: Arc<Config>,
    pending_transactions: Arc<Mutex<HashMap<String, PendingTransaction>>>,
    address_balances: Arc<Mutex<HashMap<String, u64>>>,
    notified_transactions: Arc<Mutex<HashSet<String>>>,
    notified_transactions_times: Arc<Mutex<HashMap<String, Instant>>>,
}

impl OutgoingTransactionHandler {
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

    #[allow(clippy::too_many_arguments)]
    pub async fn handle_outgoing_tx(
        &self,
        address: &str,
        chat_id: i64,
        txid: String,
        sent_amount: u64, // Amount sent to recipients (for display)
        net_change: u64,  // Net change for balance (removed - added = sent + fee - change)
        tx_daa_score: u64,
        virtual_daa_score: u64,
    ) -> Result<()> {
        // Check if we've already notified about this transaction (early exit to avoid duplicate work)
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
        if should_store_outgoing_pending(is_in_block, daa_diff, confirmation_depth) {
            // Not yet confirmed, store as pending
            info!(
                "Outgoing transaction {} stored as pending (DAA diff: {} < {}, in_mempool: true, will be processed when confirmed)",
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
                        amount: -(sent_amount as i64), // Store sent amount for display
                        net_change: Some(net_change),  // Store net change for balance calculation
                        daa_score: tx_daa_score,
                        is_incoming: false,
                    },
                );
                info!(
                    "Outgoing transaction {} added to pending queue (chat_id: {}, address: {})",
                    txid, chat_id, address
                );
            } else {
                info!(
                    "Outgoing transaction {} already in pending queue",
                    txid
                );
            }
            return Ok(());
        }

        // Transaction is confirmed and not yet notified
        info!(
            "Outgoing transaction confirmed: {} (DAA diff: {}, in_block: {})",
            txid, daa_diff, is_in_block
        );

        // Check if already notified (early exit to prevent duplicate work)
        // IMPORTANT: We check but DON'T mark yet - we'll mark AFTER successfully sending
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

        // Get transaction details for fee and timestamp
        // NOTE: The amount parameter should already be the actual sent amount (calculated from outputs)
        // if the transaction was in mempool when we processed UtxosChanged. If not, it's the UTXO net.
        let (fee, timestamp) = match mempool_entry_opt {
            Some(entry) => {
                let tx = &entry.transaction;

                // Calculate fee from transaction data
                let fee = self
                    .kaspa_client
                    .calculate_transaction_fee(tx)
                    .await
                    .unwrap_or_else(|| {
                        // If fee calculation fails, estimate as 0.001 KAS (100,000 sompi) as typical minimum
                        warn!(
                            "Could not calculate fee for transaction {}, using default estimate",
                            txid
                        );
                        100_000 // 0.001 KAS default estimate
                    });

                // Get timestamp - block_time is in milliseconds
                let ts = tx
                    .verbose_data
                    .as_ref()
                    .map(|vd| vd.block_time)
                    .unwrap_or_else(|| chrono::Utc::now().timestamp_millis() as u64);

                (fee, ts)
            }
            None => {
                // Transaction not in mempool (already confirmed)
                // CRITICAL ISSUE: When transaction is confirmed, we can't get it from mempool
                // The UTXO calculation (removed - added) should give us the net amount sent
                // But if it's showing the change amount instead, there's a bug in the UTXO calculation
                //
                // The UTXO calculation should be:
                // - removed_amount = total inputs spent from tracked address
                // - added_amount = change returned to tracked address
                // - net_outgoing = removed_amount - added_amount = amount sent + fee
                //
                // If we're seeing the change amount (155 KAS) instead of sent amount (12 KAS),
                // it means the UTXO calculation is wrong or we're using added_amount instead of net
                //
                // TODO: Add method to get transaction from block to calculate from outputs
                // For now, we have to use the UTXO calculation which may be incorrect
                warn!(
                    "Transaction {} not found in mempool (already confirmed). \
                    Using sent_amount: {} sompi, net_change: {} sompi. \
                    WARNING: If transaction was already confirmed when first processed, amounts may be incorrect.",
                    txid, sent_amount, net_change
                );
                (100_000, chrono::Utc::now().timestamp_millis() as u64) // 0.001 KAS default
            }
        };

        // CRITICAL: Always fetch balance from blockchain to ensure accuracy
        // The blockchain balance already includes this transaction, so we calculate previous balance
        // For outgoing transactions: current_balance = previous_balance - net_change
        // So: previous_balance = current_balance + net_change
        // 
        // IMPORTANT: net_change represents the amount that left the address:
        // net_change = removed_amount - added_amount = (amount_sent + fee) - change_returned
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

        // Calculate previous balance: current balance already reflects this outgoing transaction
        // net_change = removed_amount - added_amount = (amount_sent + fee) - change_returned
        // For outgoing: previous_balance = current_balance + net_change
        // Validate that net_change makes sense (should be > 0 for outgoing transactions, unless it's a self-transfer)
        if net_change == 0 && sent_amount > 0 {
            warn!(
                "WARNING: net_change is 0 but sent_amount is {} for transaction {} - this might indicate a self-transfer or calculation error",
                sent_amount, txid
            );
        }
        
        let (previous_balance, new_balance) =
            calculate_outgoing_balances(current_balance, net_change);
        
        // Validate balance calculation makes sense
        if previous_balance < new_balance {
            warn!(
                "WARNING: Balance calculation error for transaction {}: previous_balance ({}) < new_balance ({})",
                txid, previous_balance, new_balance
            );
        }

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
            "Outgoing transaction balance for {}: previous = {} sompi, net_change = {} sompi, new = {} sompi, sent_amount = {} sompi, fee = {} sompi",
            address, previous_balance, net_change, new_balance, sent_amount, fee
        );

        // CRITICAL: Send notification to the specific user
        // sent_amount is the amount sent to recipients (calculated from outputs, for display)
        // net_change is used for balance calculation (removed - added)
        info!(
            "Sending outgoing transaction notification to chat {} for txid {} (sent: {} KAS, fee: {} KAS, prev_balance: {} KAS, new_balance: {} KAS)",
            chat_id, 
            txid,
            KaspaClient::sompi_to_kas(sent_amount),
            KaspaClient::sompi_to_kas(fee),
            KaspaClient::sompi_to_kas(previous_balance),
            KaspaClient::sompi_to_kas(new_balance)
        );
        let notification_result = self
            .telegram_client
            .send_outgoing_tx_to_chat(
                chat_id,
                address,
                KaspaClient::sompi_to_kas(sent_amount),
                KaspaClient::sompi_to_kas(fee),
                KaspaClient::sompi_to_kas(previous_balance),
                KaspaClient::sompi_to_kas(new_balance),
                &txid,
                tx_daa_score,
                timestamp,
            )
            .await;
        
        match notification_result {
            Ok(_) => {
                info!(
                    "✅ Successfully sent outgoing transaction notification to chat {} for txid {}",
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
                // Log the full error details for debugging
                error!(
                    "Notification failure details - chat_id: {}, address: {}, txid: {}, error: {:?}",
                    chat_id, address, txid, e
                );
                // IMPORTANT: Don't mark as notified if notification failed
                // This allows the transaction to be retried on the next UtxosChanged notification
                // Return error so the caller knows it failed
                return Err(anyhow::anyhow!(
                    "Failed to send outgoing transaction notification: {}",
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
    use super::{calculate_outgoing_balances, should_store_outgoing_pending};

    #[test]
    fn outgoing_pending_gate_respects_block_and_depth() {
        assert!(should_store_outgoing_pending(false, 2, 10));
        assert!(!should_store_outgoing_pending(true, 0, 10));
        assert!(!should_store_outgoing_pending(false, 10, 10));
        assert!(!should_store_outgoing_pending(false, 11, 10));
    }

    #[test]
    fn outgoing_balance_math_is_consistent() {
        let (prev, new) = calculate_outgoing_balances(1_000, 250);
        assert_eq!(prev, 1_250);
        assert_eq!(new, 1_000);
    }

    #[test]
    fn outgoing_balance_math_handles_zero_change() {
        let (prev, new) = calculate_outgoing_balances(1_000, 0);
        assert_eq!(prev, 1_000);
        assert_eq!(new, 1_000);
    }
}
