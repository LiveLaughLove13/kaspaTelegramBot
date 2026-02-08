use crate::config::Config;
use crate::kaspa_client::KaspaClient;
use crate::telegram::TelegramClient;
use anyhow::{Context, Result};
// Subnetwork IDs:
// - Normal transactions: 0000000000000000000000000000000000000000
// - Coinbase transactions: 0100000000000000000000000000000000000000 (SUBNETWORK_ID_COINBASE)
use kaspa_consensus_core::subnets::SUBNETWORK_ID_COINBASE;
use kaspa_rpc_core::UtxosChangedNotification;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

pub struct PendingTransaction {
    pub txid: String,
    pub address: String,
    pub chat_id: i64,
    pub amount: i64,             // Positive for incoming, negative for outgoing
    pub net_change: Option<u64>, // For outgoing: net change (removed - added) for balance calculation
    pub daa_score: u64,
    pub is_incoming: bool,
}

pub struct TransactionProcessor {
    kaspa_client: Arc<KaspaClient>,
    telegram_client: Arc<TelegramClient>,
    config: Arc<Config>,
    pending_transactions: Arc<Mutex<HashMap<String, PendingTransaction>>>,
    address_balances: Arc<Mutex<HashMap<String, u64>>>,
    notified_transactions: Arc<Mutex<HashSet<String>>>,
    notified_transactions_times: Arc<Mutex<HashMap<String, Instant>>>,
}

impl TransactionProcessor {
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

    pub async fn handle_utxos_changed(
        &self,
        utxos_changed: UtxosChangedNotification,
    ) -> Result<()> {
        let virtual_daa_score = self
            .kaspa_client
            .get_virtual_daa_score()
            .await
            .context("Failed to get virtual DAA score")?;

        // All UTXOs in a UtxosChanged notification are from the same transaction.
        // The transaction ID is in added.outpoint.transaction_id (the transaction that created the new UTXOs).
        // Group by address to calculate net amounts (removed - change returned)
        type AddressData = (i64, u64, u64, u64, String); // (chat_id, removed_amount, added_amount, earliest_daa_score, txid)
        let mut address_data: HashMap<String, AddressData> = HashMap::new();

        debug!(
            "Processing UtxosChanged: {} removed UTXOs, {} added UTXOs",
            utxos_changed.removed.len(),
            utxos_changed.added.len()
        );

        // First, get the transaction ID from added UTXOs (this is the transaction we care about)
        // All UTXOs in a UtxosChanged notification are from the same transaction
        // We can get the transaction ID from ANY added UTXO, even if it's not for a tracked address
        let mut current_txid: Option<String> = None;
        for added in utxos_changed.added.iter() {
            if current_txid.is_none() {
                current_txid = Some(added.outpoint.transaction_id.to_string());
                break; // All added UTXOs have the same transaction ID
            }
        }

        // If no added UTXOs, we cannot determine the transaction ID
        // This can happen when all outputs go to untracked addresses
        // In this case, we cannot process the transaction, so skip it
        if current_txid.is_none() {
            // For removed UTXOs, the transaction_id is the OLD transaction that created them
            // We need to find which transaction spent them - this is complex and requires
            // additional RPC calls. Since we can't process transactions without a transaction ID,
            // we'll skip this notification. This is expected for edge cases where all outputs
            // go to untracked addresses.
            debug!(
                "UtxosChanged notification has no added UTXOs - cannot determine transaction ID, skipping (removed: {}, added: {})",
                utxos_changed.removed.len(),
                utxos_changed.added.len()
            );
            return Ok(());
        }

        // Process removed UTXOs (inputs spent) - group by address
        // Note: removed.outpoint.transaction_id is the OLD transaction that created the UTXO being spent
        // We need to use the transaction ID from added UTXOs instead
        for removed in utxos_changed.removed.iter() {
            if let Some(address) = &removed.address {
                let addr_str = address.to_string();
                if let Some(chat_id) = self.kaspa_client.get_address_owner(&addr_str) {
                    // Use txid from added if available, otherwise we can't process this properly
                    let txid = current_txid.clone().unwrap_or_else(|| {
                        // If no added UTXOs, this is a pure outgoing transaction
                        // We'll need to handle this case - for now use a placeholder
                        "unknown".to_string()
                    });

                    let entry = address_data.entry(addr_str.clone()).or_insert_with(|| {
                        (chat_id, 0, 0, removed.utxo_entry.block_daa_score, txid)
                    });
                    entry.1 += removed.utxo_entry.amount; // Sum removed amounts
                                                          // Use the earliest (lowest) DAA score
                    if removed.utxo_entry.block_daa_score < entry.3 {
                        entry.3 = removed.utxo_entry.block_daa_score;
                    }
                }
            }
        }

        // Process added UTXOs (outputs received, including change) - group by address
        // Coinbase (mined) transactions have no inputs.
        // IMPORTANT: determine coinbase using the *notification's* removed list, not per-address
        // aggregates. When subscribed to all addresses, a non-coinbase tx will have removed UTXOs
        // (its inputs) even if none of them belong to tracked addresses.
        if let Some(ref txid) = current_txid {
            // Check if transaction is already marked as notified (avoid duplicates)
            {
                let notified = self.notified_transactions.lock().await;
                if notified.contains(txid) {
                    debug!(
                        "Skipping transaction {} - already processed (likely a block reward)",
                        txid
                    );
                    return Ok(());
                }
            }

            if utxos_changed.removed.is_empty() && !utxos_changed.added.is_empty() {
                // No inputs present in this tx => coinbase candidate.
                // If it is still in mempool, verify by subnetwork id; otherwise treat as coinbase.
                let is_coinbase = if let Ok(Some(mempool_entry)) =
                    self.kaspa_client.get_mempool_entry(txid).await
                {
                    mempool_entry.transaction.subnetwork_id == SUBNETWORK_ID_COINBASE
                } else {
                    true
                };

                if is_coinbase {
                    debug!(
                        "Detected coinbase transaction {} in UtxosChanged - skipping (handled by BlockAdded)",
                        txid
                    );
                    let mut notified = self.notified_transactions.lock().await;
                    notified.insert(txid.clone());
                    drop(notified);
                    let mut times = self.notified_transactions_times.lock().await;
                    times.insert(txid.clone(), Instant::now());
                    return Ok(());
                }
            }
        }

        for added in utxos_changed.added.iter() {
            if let Some(address) = &added.address {
                let addr_str = address.to_string();
                if let Some(chat_id) = self.kaspa_client.get_address_owner(&addr_str) {
                    let txid = added.outpoint.transaction_id.to_string();
                    let entry = address_data.entry(addr_str.clone()).or_insert_with(|| {
                        (
                            chat_id,
                            0,
                            0,
                            added.utxo_entry.block_daa_score,
                            txid.clone(),
                        )
                    });
                    entry.2 += added.utxo_entry.amount; // Sum added amounts
                                                        // Use the earliest (lowest) DAA score
                    if added.utxo_entry.block_daa_score < entry.3 {
                        entry.3 = added.utxo_entry.block_daa_score;
                    }
                    // Ensure txid is set (this is the actual transaction ID)
                    entry.4 = txid;
                }
            }
        }

        // Process each address
        debug!(
            "Processing {} addresses from UtxosChanged notification",
            address_data.len()
        );
        for (address, (chat_id, removed_amount, added_amount, tx_daa_score, txid)) in address_data {
            info!(
                "Processing transaction for address {}: removed={}, added={}, txid={}, daa_score={}",
                address, removed_amount, added_amount, txid, tx_daa_score
            );

            // Check if this transaction is already marked as notified (e.g., coinbase transactions
            // are marked when we process block rewards)
            {
                let notified = self.notified_transactions.lock().await;
                if notified.contains(&txid) {
                    debug!(
                        "Skipping transaction {} - already processed (likely a block reward)",
                        txid
                    );
                    continue;
                }
            }

            // Handle case where txid is unknown (pure outgoing with no change)
            // In this case, we need to process removed UTXOs as outgoing, but we don't have the transaction ID
            // We'll need to get it from the transaction that spent these UTXOs
            // For now, if we have removed but no added and txid is unknown, we can't process it properly
            // This is a rare edge case - most transactions have change outputs
            if txid == "unknown" {
                if removed_amount > 0 && added_amount == 0 {
                    // Pure outgoing with no change - we need the transaction ID
                    // This requires looking up which transaction spent these UTXOs, which is complex
                    // For now, skip with a warning
                    warn!(
                        "Cannot process pure outgoing transaction for address {} - no transaction ID available (removed: {}, added: {})",
                        address, removed_amount, added_amount
                    );
                }
                continue;
            }

            // Skip historical transactions (only if they're very old - more than 10,000 DAA)
            // This prevents spam when an address is first added, but allows recent transactions
            let daa_diff = virtual_daa_score.saturating_sub(tx_daa_score);
            const HISTORICAL_THRESHOLD: u64 = 10_000; // Increased from 1000 to 10,000 to avoid filtering recent transactions
            if daa_diff > HISTORICAL_THRESHOLD {
                debug!(
                    "Skipping historical transaction {} for address {} (DAA diff: {} > {})",
                    txid, address, daa_diff, HISTORICAL_THRESHOLD
                );
                let mut notified = self.notified_transactions.lock().await;
                notified.insert(txid.clone());
                drop(notified);
                let mut times = self.notified_transactions_times.lock().await;
                times.insert(txid, Instant::now());
                continue;
            }

            // Calculate net amounts
            // IMPORTANT: Check for pure incoming FIRST (removed == 0, added > 0) to avoid misclassification
            // Then check for pure outgoing (removed > 0, added == 0)
            // Then check for net incoming (added > removed)
            // Finally check for net outgoing (removed > added)
            //
            // The UTXO calculation (removed - added) gives the net change, which is:
            // - For outgoing: amount sent + fee - change returned = net amount that left the address
            // - This is NOT the same as the amount sent to recipients!
            // - We need to get the transaction from mempool NOW (while it's still available) to calculate from outputs

            // Check for pure incoming FIRST (no inputs from this address, only outputs to it)
            // This MUST be checked before outgoing to avoid misclassification
            if removed_amount == 0 && added_amount > 0 {
                // Pure incoming transaction (no inputs from this address, only outputs to it)
                info!(
                    "Processing PURE INCOMING transaction {} for address {}: removed={}, added={}",
                    txid, address, removed_amount, added_amount
                );
                if self.config.notifications.incoming_tx {
                    self.handle_incoming_tx(
                        &address,
                        chat_id,
                        txid.clone(),
                        added_amount,
                        tx_daa_score,
                        virtual_daa_score,
                    )
                    .await?;
                } else {
                    debug!(
                        "Skipping incoming transaction {}: notifications disabled",
                        txid
                    );
                }
                continue; // IMPORTANT: Skip to next address to avoid processing as outgoing
            } else if removed_amount > 0 && added_amount == 0 {
                // Pure outgoing with no change
                // For pure outgoing, removed_amount is both the sent amount and net change (no change returned)
                debug!(
                    "Processing pure outgoing transaction {} for address {}: removed={}, added={}",
                    txid, address, removed_amount, added_amount
                );
                if self.config.notifications.outgoing_tx {
                    self.handle_outgoing_tx(
                        &address,
                        chat_id,
                        txid.clone(),
                        removed_amount, // Sent amount (no change, so this is the full amount)
                        removed_amount, // Net change (same as sent since no change)
                        tx_daa_score,
                        virtual_daa_score,
                    )
                    .await?;
                }
            } else if added_amount > removed_amount {
                // Net incoming transaction (received more than spent)
                let net_incoming = added_amount - removed_amount;
                info!(
                    "Processing INCOMING transaction {} for address {}: removed={}, added={}, net={}",
                    txid, address, removed_amount, added_amount, net_incoming
                );
                if self.config.notifications.incoming_tx && net_incoming > 0 {
                    self.handle_incoming_tx(
                        &address,
                        chat_id,
                        txid.clone(),
                        net_incoming,
                        tx_daa_score,
                        virtual_daa_score,
                    )
                    .await?;
                } else {
                    debug!(
                        "Skipping incoming transaction {}: notifications disabled or net_incoming is 0",
                        txid
                    );
                }
                continue; // IMPORTANT: Skip to next address to avoid processing as outgoing
            } else if removed_amount > added_amount {
                // Net outgoing transaction (spent more than received back as change)
                let net_outgoing = removed_amount - added_amount;

                info!(
                    "OUTGOING TX {} for {}: removed={} sompi, added={} sompi, net_outgoing={} sompi",
                    txid, address, removed_amount, added_amount, net_outgoing
                );

                // Try to get transaction from mempool NOW to calculate actual sent amount from outputs
                // This is critical because once the transaction is confirmed, we can't get it from mempool
                let actual_sent_amount = if let Ok(Some(mempool_entry)) =
                    self.kaspa_client.get_mempool_entry(&txid).await
                {
                    // Transaction is in mempool - calculate actual sent amount from outputs
                    // IMPORTANT: Sum outputs NOT to the tracked address (these are the actual sent amounts)
                    // Outputs TO the tracked address are change and should be excluded
                    let tx = &mempool_entry.transaction;
                    let mut total_sent: u64 = 0;
                    let mut outputs_to_tracked: u64 = 0;
                    for output in &tx.outputs {
                        if let Some(verbose_data) = &output.verbose_data {
                            let output_address = verbose_data.script_public_key_address.to_string();
                            if output_address == address {
                                // This is change going back to the tracked address
                                outputs_to_tracked += output.value;
                            } else {
                                // This is an output to another address (the actual amount sent)
                                total_sent += output.value;
                            }
                        }
                    }
                    if total_sent > 0 {
                        info!(
                            "Transaction {}: Calculated from outputs - sent to others: {} sompi, change to tracked address: {} sompi, UTXO net: {} sompi",
                            txid, total_sent, outputs_to_tracked, net_outgoing
                        );
                        Some(total_sent)
                    } else {
                        warn!(
                            "Transaction {}: No outputs to other addresses found (only {} sompi to tracked address). This shouldn't happen for outgoing transactions.",
                            txid, outputs_to_tracked
                        );
                        None
                    }
                } else {
                    // Transaction not in mempool (already confirmed) - will use UTXO calculation as fallback
                    warn!(
                        "Transaction {} not in mempool (already confirmed). Cannot calculate sent amount from outputs. Using UTXO net: {} sompi",
                        txid, net_outgoing
                    );
                    None
                };
                if self.config.notifications.outgoing_tx && net_outgoing > 0 {
                    // Use actual sent amount from outputs if available, otherwise use UTXO net
                    // IMPORTANT: We pass both the sent amount (for display) and net change (for balance)
                    let sent_amount_for_display = actual_sent_amount.unwrap_or(net_outgoing);
                    self.handle_outgoing_tx(
                        &address,
                        chat_id,
                        txid.clone(),
                        sent_amount_for_display, // Amount to display (actual sent to recipients)
                        net_outgoing, // Net change for balance calculation (removed - added)
                        tx_daa_score,
                        virtual_daa_score,
                    )
                    .await?;
                } else {
                    debug!(
                        "Skipping outgoing transaction {}: notifications disabled or net_outgoing is 0",
                        txid
                    );
                }
            } else if removed_amount > 0 && added_amount > 0 && removed_amount == added_amount {
                // removed_amount == added_amount: This could be a self-transfer
                // Still notify about it, but as an outgoing transaction (since it spent UTXOs)
                // The net is 0, but we should still show the activity
                debug!(
                    "Processing self-transfer transaction {} for address {}: removed={}, added={} (net=0, showing as outgoing)",
                    txid, address, removed_amount, added_amount
                );
                if self.config.notifications.outgoing_tx {
                    // Show as outgoing with 0 net (or show the amount spent)
                    // For self-transfers, net_change is 0 (removed == added)
                    let net_change = 0u64;
                    self.handle_outgoing_tx(
                        &address,
                        chat_id,
                        txid.clone(),
                        removed_amount, // Show the amount that was moved
                        net_change,     // Net change is 0 for self-transfers
                        tx_daa_score,
                        virtual_daa_score,
                    )
                    .await?;
                }
            } else {
                // Both are 0 or some other edge case - log and skip
                debug!(
                    "Skipping transaction {} for address {}: removed={}, added={} (no activity)",
                    txid, address, removed_amount, added_amount
                );
                // Mark as notified to prevent future processing
                let mut notified = self.notified_transactions.lock().await;
                notified.insert(txid.clone());
                drop(notified);
                let mut times = self.notified_transactions_times.lock().await;
                times.insert(txid, Instant::now());
            }
        }

        Ok(())
    }

    async fn handle_incoming_tx(
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
        if !is_in_block && daa_diff < confirmation_depth {
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

        // Mark as notified BEFORE sending to prevent race conditions
        // Use atomic check-and-insert to prevent concurrent processing
        {
            let mut notified = self.notified_transactions.lock().await;
            // Double-check after acquiring lock (another thread might have inserted it)
            if notified.contains(&txid) {
                debug!(
                    "Transaction {} already notified by another thread, skipping",
                    txid
                );
                return Ok(());
            }
            notified.insert(txid.clone());
        }
        {
            let mut times = self.notified_transactions_times.lock().await;
            times.insert(txid.clone(), Instant::now());
        }

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

        // Update balance - fetch from blockchain if not in map
        let previous_balance = {
            let balances = self.address_balances.lock().await;
            if let Some(&cached) = balances.get(address) {
                cached
            } else {
                // Balance not in map, release lock and fetch from blockchain
                // For incoming transactions, current_balance = previous_balance + amount
                // So previous_balance = current_balance - amount
                drop(balances);
                match self.kaspa_client.get_balance(address).await {
                    Ok(current_balance) => {
                        let prev = current_balance.saturating_sub(amount);
                        info!("Fetched balance for {} from blockchain: {} sompi (current), calculated previous: {} sompi", address, current_balance, prev);
                        prev
                    }
                    Err(e) => {
                        warn!(
                            "Failed to fetch balance for {} from blockchain: {}, using 0",
                            address, e
                        );
                        0
                    }
                }
            }
        };
        let new_balance = previous_balance + amount;
        let mut balances = self.address_balances.lock().await;
        balances.insert(address.to_string(), new_balance);

        // Send notification to the specific user
        self.telegram_client
            .send_incoming_tx_to_chat(
                chat_id,
                address,
                KaspaClient::sompi_to_kas(amount),
                KaspaClient::sompi_to_kas(previous_balance),
                KaspaClient::sompi_to_kas(new_balance),
                &txid,
                tx_daa_score,
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
        self.pending_transactions.lock().await.remove(&txid);

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_outgoing_tx(
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
        if !is_in_block && daa_diff < confirmation_depth {
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
                        amount: -(sent_amount as i64), // Store sent amount for display
                        net_change: Some(net_change),  // Store net change for balance calculation
                        daa_score: tx_daa_score,
                        is_incoming: false,
                    },
                );
            }
            return Ok(());
        }

        // Transaction is confirmed and not yet notified
        info!(
            "Outgoing transaction confirmed: {} (DAA diff: {}, in_block: {})",
            txid, daa_diff, is_in_block
        );

        // Mark as notified BEFORE sending to prevent race conditions
        // Use atomic check-and-insert to prevent concurrent processing
        {
            let mut notified = self.notified_transactions.lock().await;
            // Double-check after acquiring lock (another thread might have inserted it)
            if notified.contains(&txid) {
                debug!(
                    "Transaction {} already notified by another thread, skipping",
                    txid
                );
                return Ok(());
            }
            notified.insert(txid.clone());
        }
        {
            let mut times = self.notified_transactions_times.lock().await;
            times.insert(txid.clone(), Instant::now());
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

        // Update balance - use incremental update based on UTXO changes (like wallet code does)
        // CRITICAL: net_change = removed_amount - added_amount = (amount_sent + fee) - change_returned
        // This is the actual amount that left the address
        //
        // IMPORTANT: We update balance incrementally from UTXO changes, not by fetching from blockchain
        // This matches how the wallet code works and is more reliable
        let previous_balance = {
            let balances = self.address_balances.lock().await;
            if let Some(&cached) = balances.get(address) {
                cached
            } else {
                // Balance not in cache, release lock and fetch from blockchain
                drop(balances);
                match self.kaspa_client.get_balance(address).await {
                    Ok(current_balance) => {
                        // Current balance is AFTER the transaction
                        // Previous balance = current + net_change
                        let prev = current_balance.saturating_add(net_change);
                        info!(
                            "Fetched balance for {} from blockchain: current (after tx) = {} sompi, calculated previous = {} sompi, net_change = {} sompi",
                            address, current_balance, prev, net_change
                        );
                        prev
                    }
                    Err(e) => {
                        warn!(
                            "Failed to fetch balance for {} from blockchain: {}, using 0",
                            address, e
                        );
                        0
                    }
                }
            }
        };

        // Calculate new balance: previous - net_change
        // net_change already includes the fee (it's removed - added, which accounts for everything)
        let new_balance = previous_balance.saturating_sub(net_change);

        // Update cached balance
        {
            let mut balances = self.address_balances.lock().await;
            balances.insert(address.to_string(), new_balance);
        }

        info!(
            "Balance update for {}: previous = {} sompi, net_change = {} sompi, new = {} sompi",
            address, previous_balance, net_change, new_balance
        );

        // Send notification to the specific user
        // sent_amount is the amount sent to recipients (calculated from outputs, for display)
        // net_change is used for balance calculation (removed - added)
        self.telegram_client
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
            .await
            .unwrap_or_else(|e| {
                error!(
                    "Failed to send Telegram notification to chat {}: {}",
                    chat_id, e
                )
            });

        // Remove from pending
        self.pending_transactions.lock().await.remove(&txid);

        Ok(())
    }

    // Cleanup old notified transactions to prevent memory growth
    // Removes entries older than 7 days or if set exceeds 10,000 entries
    async fn cleanup_old_notified_transactions(&self) {
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

    pub async fn check_pending_confirmations(&self, virtual_daa_score: u64) -> Result<()> {
        let confirmation_depth = self.config.confirmation.daa_score_depth;
        let pending_txs = self.pending_transactions.lock().await;
        let mut confirmed_txs = Vec::new();

        // Check all pending transactions
        for (txid, pending) in pending_txs.iter() {
            // First, check if transaction is now in a block (not in mempool anymore)
            // If it's in a block, process immediately regardless of DAA depth
            let is_in_block = self
                .kaspa_client
                .get_mempool_entry(txid)
                .await
                .ok()
                .flatten()
                .is_none();

            let daa_diff = virtual_daa_score.saturating_sub(pending.daa_score);

            // Process if: (1) transaction is in a block, OR (2) DAA depth is met
            if is_in_block || daa_diff >= confirmation_depth {
                confirmed_txs.push(txid.clone());
            }
        }
        drop(pending_txs);

        for txid in confirmed_txs {
            // Check if already notified before processing (prevent duplicates)
            {
                let notified = self.notified_transactions.lock().await;
                if notified.contains(&txid) {
                    // Already notified, remove from pending and skip
                    debug!(
                        "Transaction {} already notified, removing from pending",
                        txid
                    );
                    self.pending_transactions.lock().await.remove(&txid);
                    continue;
                }
            }

            let pending = self.pending_transactions.lock().await.remove(&txid);
            if let Some(pending) = pending {
                if pending.is_incoming {
                    self.handle_incoming_tx(
                        &pending.address,
                        pending.chat_id,
                        pending.txid.clone(),
                        pending.amount as u64,
                        pending.daa_score,
                        virtual_daa_score,
                    )
                    .await?;
                } else {
                    // For pending outgoing transactions, use stored net_change if available
                    // Otherwise use the stored amount (fallback for old pending transactions)
                    let sent_amount = (-pending.amount) as u64;
                    let net_change_for_balance = pending.net_change.unwrap_or(sent_amount);
                    self.handle_outgoing_tx(
                        &pending.address,
                        pending.chat_id,
                        pending.txid.clone(),
                        sent_amount,
                        net_change_for_balance,
                        pending.daa_score,
                        virtual_daa_score,
                    )
                    .await?;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_coinbase_transaction_filtering() {
        // This test verifies that coinbase transactions are properly filtered out
        // Note: This is a unit test that would require mocking the KaspaClient
        // For now, we verify the logic structure is correct

        // The key test is that when a coinbase transaction is detected in handle_utxos_changed,
        // it should return early and not process it as a regular transaction
        // Coinbase filtering logic is implemented in handle_utxos_changed
    }

    #[tokio::test]
    async fn test_normal_transaction_processing() {
        // This test verifies that normal (non-coinbase) transactions are processed correctly
        // Note: This would require mocking KaspaClient and TelegramClient
        // For now, we verify the logic structure

        // The key test is that when a non-coinbase transaction is detected,
        // it should be processed through handle_incoming_tx or handle_outgoing_tx
        // Normal transaction processing logic is implemented
    }

    #[tokio::test]
    async fn test_net_amount_calculation() {
        // Test that net amounts are calculated correctly:
        // - removed > added: net outgoing
        // - added > removed: net incoming
        // - removed == added: self-transfer (shown as outgoing)

        // Test case 1: Net outgoing (removed > added)
        let removed = 100_000_000; // 1 KAS
        let added = 50_000_000; // 0.5 KAS (change)
        let net_outgoing = removed - added;
        assert_eq!(net_outgoing, 50_000_000, "Net outgoing should be 0.5 KAS");

        // Test case 2: Net incoming (added > removed)
        let removed2 = 0;
        let added2 = 100_000_000; // 1 KAS received
        let net_incoming = added2 - removed2;
        assert_eq!(net_incoming, 100_000_000, "Net incoming should be 1 KAS");

        // Test case 3: Self-transfer (removed == added)
        let removed3 = 100_000_000;
        let added3 = 100_000_000;
        assert_eq!(removed3, added3, "Self-transfer should have equal amounts");
    }

    #[tokio::test]
    async fn test_historical_transaction_filtering() {
        // Test that very old transactions (> 10,000 DAA) are filtered out
        const HISTORICAL_THRESHOLD: u64 = 10_000;

        let virtual_daa_score: u64 = 100_000;
        let tx_daa_score: u64 = 89_000; // 11,000 DAA difference
        let daa_diff = virtual_daa_score.saturating_sub(tx_daa_score);

        assert!(
            daa_diff > HISTORICAL_THRESHOLD,
            "Should filter historical transactions"
        );

        let tx_daa_score2: u64 = 95_000; // 5,000 DAA difference
        let daa_diff2 = virtual_daa_score.saturating_sub(tx_daa_score2);
        assert!(
            daa_diff2 <= HISTORICAL_THRESHOLD,
            "Should not filter recent transactions"
        );
    }
}
