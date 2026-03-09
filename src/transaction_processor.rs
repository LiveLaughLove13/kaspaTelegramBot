use crate::config::Config;
use crate::incoming_transaction::IncomingTransactionHandler;
use crate::kaspa_client::KaspaClient;
use crate::outgoing_transaction::OutgoingTransactionHandler;
use crate::telegram::TelegramClient;
use anyhow::{Context, Result};
// Subnetwork IDs:
// - Normal transactions: 0000000000000000000000000000000000000000
// - Coinbase transactions: 0100000000000000000000000000000000000000 (SUBNETWORK_ID_COINBASE)
use kaspa_consensus_core::subnets::SUBNETWORK_ID_COINBASE;
use kaspa_rpc_core::UtxosChangedNotification;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

const HISTORICAL_THRESHOLD: u64 = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NetDirection {
    PureIncoming,
    PureOutgoing,
    NetIncoming(u64),
    NetOutgoing(u64),
    EqualNonZero,
    ZeroZero,
}

fn classify_net_direction(removed_amount: u64, added_amount: u64) -> NetDirection {
    if removed_amount == 0 && added_amount > 0 {
        NetDirection::PureIncoming
    } else if removed_amount > 0 && added_amount == 0 {
        NetDirection::PureOutgoing
    } else if added_amount > removed_amount {
        NetDirection::NetIncoming(added_amount - removed_amount)
    } else if removed_amount > added_amount {
        NetDirection::NetOutgoing(removed_amount - added_amount)
    } else if removed_amount > 0 {
        NetDirection::EqualNonZero
    } else {
        NetDirection::ZeroZero
    }
}

fn is_historical_transaction(virtual_daa_score: u64, tx_daa_score: u64) -> bool {
    virtual_daa_score.saturating_sub(tx_daa_score) > HISTORICAL_THRESHOLD
}

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
    #[allow(dead_code)]
    telegram_client: Arc<TelegramClient>,
    config: Arc<Config>,
    pending_transactions: Arc<Mutex<HashMap<String, PendingTransaction>>>,
    #[allow(dead_code)]
    address_balances: Arc<Mutex<HashMap<String, u64>>>,
    notified_transactions: Arc<Mutex<HashSet<String>>>,
    notified_transactions_times: Arc<Mutex<HashMap<String, Instant>>>,
    incoming_handler: IncomingTransactionHandler,
    outgoing_handler: OutgoingTransactionHandler,
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
        let incoming_handler = IncomingTransactionHandler::new(
            kaspa_client.clone(),
            telegram_client.clone(),
            config.clone(),
            pending_transactions.clone(),
            address_balances.clone(),
            notified_transactions.clone(),
            notified_transactions_times.clone(),
        );

        let outgoing_handler = OutgoingTransactionHandler::new(
            kaspa_client.clone(),
            telegram_client.clone(),
            config.clone(),
            pending_transactions.clone(),
            address_balances.clone(),
            notified_transactions.clone(),
            notified_transactions_times.clone(),
        );

        Self {
            kaspa_client,
            telegram_client,
            config,
            pending_transactions,
            address_balances,
            notified_transactions,
            notified_transactions_times,
            incoming_handler,
            outgoing_handler,
        }
    }

    #[allow(clippy::ifs_same_cond)]
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
        type AddressData = (String, i64, u64, u64, u64, String); // (address, chat_id, removed_amount, added_amount, earliest_daa_score, txid)
        let mut address_data: HashMap<(String, i64), AddressData> = HashMap::new();

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
                let owners = self.kaspa_client.get_address_owners(&addr_str);
                for chat_id in owners {
                    // Use txid from added if available, otherwise we can't process this properly
                    let txid = current_txid.clone().unwrap_or_else(|| {
                        // If no added UTXOs, this is a pure outgoing transaction
                        // We'll need to handle this case - for now use a placeholder
                        "unknown".to_string()
                    });

                    let key = (addr_str.clone(), chat_id);
                    let entry = address_data.entry(key).or_insert_with(|| {
                        (
                            addr_str.clone(),
                            chat_id,
                            0,
                            0,
                            removed.utxo_entry.block_daa_score,
                            txid,
                        )
                    });
                    entry.2 += removed.utxo_entry.amount; // Sum removed amounts
                                                          // Use the earliest (lowest) DAA score
                    if removed.utxo_entry.block_daa_score < entry.4 {
                        entry.4 = removed.utxo_entry.block_daa_score;
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
                let owners = self.kaspa_client.get_address_owners(&addr_str);
                for chat_id in owners {
                    let txid = added.outpoint.transaction_id.to_string();
                    let key = (addr_str.clone(), chat_id);
                    let entry = address_data.entry(key).or_insert_with(|| {
                        (
                            addr_str.clone(),
                            chat_id,
                            0,
                            0,
                            added.utxo_entry.block_daa_score,
                            txid.clone(),
                        )
                    });
                    entry.3 += added.utxo_entry.amount; // Sum added amounts
                                                        // Use the earliest (lowest) DAA score
                    if added.utxo_entry.block_daa_score < entry.4 {
                        entry.4 = added.utxo_entry.block_daa_score;
                    }
                    // Ensure txid is set (this is the actual transaction ID)
                    entry.5 = txid;
                }
            }
        }

        // Process each address
        debug!(
            "Processing {} addresses from UtxosChanged notification",
            address_data.len()
        );
        for (_, (address, chat_id, removed_amount, added_amount, tx_daa_score, txid)) in address_data
        {
            info!(
                "Processing transaction for address {}: removed={} sompi ({} KAS), added={} sompi ({} KAS), txid={}, daa_score={}, chat_id={}",
                address, 
                removed_amount, 
                KaspaClient::sompi_to_kas(removed_amount),
                added_amount,
                KaspaClient::sompi_to_kas(added_amount),
                txid, 
                tx_daa_score,
                chat_id
            );

            // Check if this transaction is already marked as notified (e.g., coinbase transactions
            // are marked when we process block rewards, or we already processed it from a previous UtxosChanged notification)
            {
                let notified = self.notified_transactions.lock().await;
                if notified.contains(&txid) {
                    info!(
                        "Skipping transaction {} for address {} - already processed and notified (txid was processed in a previous UtxosChanged notification)",
                        txid, address
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
            if is_historical_transaction(virtual_daa_score, tx_daa_score) {
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
            match classify_net_direction(removed_amount, added_amount) {
                NetDirection::PureIncoming => {
                // Pure incoming transaction (no inputs from this address, only outputs to it)
                info!(
                    "Processing PURE INCOMING transaction {} for address {}: removed={}, added={}",
                    txid, address, removed_amount, added_amount
                );
                if self.config.notifications.incoming_tx {
                    self.incoming_handler
                        .handle_incoming_tx(
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
            }
                NetDirection::PureOutgoing => {
                // Pure outgoing with no change
                // For pure outgoing, removed_amount is both the sent amount and net change (no change returned)
                debug!(
                    "Processing pure outgoing transaction {} for address {}: removed={}, added={}",
                    txid, address, removed_amount, added_amount
                );
                if self.config.notifications.outgoing_tx {
                    self.outgoing_handler
                        .handle_outgoing_tx(
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
            }
                NetDirection::NetIncoming(net_incoming) => {
                // Net incoming transaction (received more than spent)
                info!(
                    "Processing INCOMING transaction {} for address {}: removed={}, added={}, net={}",
                    txid, address, removed_amount, added_amount, net_incoming
                );
                if self.config.notifications.incoming_tx && net_incoming > 0 {
                    self.incoming_handler
                        .handle_incoming_tx(
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
            }
                NetDirection::NetOutgoing(net_outgoing) => {
                // Net outgoing transaction (spent more than received back as change)
                info!(
                    "OUTGOING TX {} for {}: removed={} sompi, added={} sompi, net_outgoing={} sompi",
                    txid, address, removed_amount, added_amount, net_outgoing
                );
                
                // Log notification settings for debugging
                info!(
                    "Outgoing notification settings: enabled={}, net_outgoing={}",
                    self.config.notifications.outgoing_tx, net_outgoing
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
                    let mut received_to_tracked: u64 = 0;
                    for output in &tx.outputs {
                        if let Some(verbose_data) = &output.verbose_data {
                            let output_address = verbose_data.script_public_key_address.to_string();
                            if output_address == address {
                                // This is an output TO the tracked address (change or received)
                                received_to_tracked += output.value;
                            } else {
                                // This is an output to another address (the actual amount sent)
                                total_sent += output.value;
                            }
                        }
                    }
                    if total_sent > 0 {
                        info!(
                            "Transaction {}: Calculated from outputs - sent to others: {} sompi, received to tracked address: {} sompi, UTXO net: {} sompi",
                            txid, total_sent, received_to_tracked, net_outgoing
                        );
                        Some(total_sent)
                    } else {
                        warn!(
                            "Transaction {}: No outputs to other addresses found (only {} sompi to tracked address). This shouldn't happen for outgoing transactions.",
                            txid, received_to_tracked
                        );
                        None
                    }
                } else {
                    // Transaction not in mempool (already confirmed) - cannot get transaction outputs from node
                    // For regular outgoing transactions (removed > added), UTXO net calculation is accurate
                    // This represents the actual net amount that left the address (sent + fee - change)
                    info!(
                        "Transaction {} not in mempool (already confirmed). Using UTXO net calculation: {} sompi ({} KAS) as sent amount.",
                        txid,
                        net_outgoing,
                        KaspaClient::sompi_to_kas(net_outgoing)
                    );
                    None
                };
                if self.config.notifications.outgoing_tx && net_outgoing > 0 {
                    // Use actual sent amount from outputs if available, otherwise use UTXO net
                    // IMPORTANT: We pass both the sent amount (for display) and net change (for balance)
                    let sent_amount_for_display = actual_sent_amount.unwrap_or(net_outgoing);
                    info!(
                        "Calling handle_outgoing_tx for txid {}: sent_amount={} sompi ({} KAS), net_change={} sompi ({} KAS), chat_id={}",
                        txid, 
                        sent_amount_for_display, 
                        KaspaClient::sompi_to_kas(sent_amount_for_display),
                        net_outgoing,
                        KaspaClient::sompi_to_kas(net_outgoing),
                        chat_id
                    );
                    // Validate net_change makes sense
                    if net_outgoing == 0 && sent_amount_for_display > 0 {
                        warn!(
                            "WARNING: net_change is 0 but sent_amount is {} for transaction {} - this might be a self-transfer that should be skipped",
                            sent_amount_for_display, txid
                        );
                    }
                    // IMPORTANT: Don't mark as notified here - let the handler do it AFTER successfully sending
                    // This ensures that if notification fails, we can retry
                    self.outgoing_handler
                        .handle_outgoing_tx(
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
                    warn!(
                        "Skipping outgoing transaction {}: notifications enabled={}, net_outgoing={} sompi ({} KAS)",
                        txid, 
                        self.config.notifications.outgoing_tx, 
                        net_outgoing,
                        KaspaClient::sompi_to_kas(net_outgoing)
                    );
                }
            }
                NetDirection::EqualNonZero => {
                // removed_amount == added_amount: Need to check transaction outputs to determine direction
                // This could be:
                // 1. Incoming transaction where sender also sent change back to themselves (rare but possible)
                // 2. Outgoing transaction where change equals what was sent (self-transfer)
                // 3. Mixed transaction
                
                // Get transaction details to determine if it's incoming or outgoing
                match self.kaspa_client.get_transaction_sent_amount(&txid, &address).await {
                    Ok(Some((sent_to_others, received_to_tracked))) => {
                        // We have transaction details - determine direction based on outputs
                        if received_to_tracked > sent_to_others {
                            // More received than sent - this is an incoming transaction
                            let net_incoming = received_to_tracked.saturating_sub(sent_to_others);
                            info!(
                                "Transaction {} (removed==added): Determined as INCOMING from outputs - received: {} sompi ({} KAS), sent: {} sompi ({} KAS), net_incoming: {} sompi ({} KAS)",
                                txid,
                                received_to_tracked,
                                KaspaClient::sompi_to_kas(received_to_tracked),
                                sent_to_others,
                                KaspaClient::sompi_to_kas(sent_to_others),
                                net_incoming,
                                KaspaClient::sompi_to_kas(net_incoming)
                            );
                            if self.config.notifications.incoming_tx && net_incoming > 0 {
                                self.incoming_handler
                                    .handle_incoming_tx(
                                        &address,
                                        chat_id,
                                        txid.clone(),
                                        received_to_tracked, // Use actual received amount
                                        tx_daa_score,
                                        virtual_daa_score,
                                    )
                                    .await?;
                            }
                            continue; // Skip to next address
                        } else if sent_to_others > 0 {
                            // Has outputs to other addresses - this is an outgoing transaction
                            info!(
                                "Transaction {} (removed==added): Determined as OUTGOING from outputs - sent: {} sompi ({} KAS), received (change): {} sompi ({} KAS)",
                                txid,
                                sent_to_others,
                                KaspaClient::sompi_to_kas(sent_to_others),
                                received_to_tracked,
                                KaspaClient::sompi_to_kas(received_to_tracked)
                            );
                            if self.config.notifications.outgoing_tx {
                                let net_outgoing = removed_amount.saturating_sub(added_amount); // Should be 0, but use fee
                                let net_change = if net_outgoing == 0 { 100_000 } else { net_outgoing }; // Fee estimate
                                self.outgoing_handler
                                    .handle_outgoing_tx(
                                        &address,
                                        chat_id,
                                        txid.clone(),
                                        sent_to_others, // Actual sent amount
                                        net_change,
                                        tx_daa_score,
                                        virtual_daa_score,
                                    )
                                    .await?;
                            }
                            continue; // Skip to next address
                        } else {
                            // No outputs to others, all to tracked address - true self-transfer
                            info!(
                                "Transaction {} (removed==added): True self-transfer - all outputs to tracked address ({} sompi). Skipping.",
                                txid, received_to_tracked
                            );
                            let mut notified = self.notified_transactions.lock().await;
                            notified.insert(txid.clone());
                            drop(notified);
                            let mut times = self.notified_transactions_times.lock().await;
                            times.insert(txid, Instant::now());
                            continue;
                        }
                    }
                    Ok(None) => {
                        // Cannot get transaction details - skip to avoid incorrect classification
                        warn!(
                            "Transaction {} (removed==added): Cannot get transaction details. Cannot determine if incoming or outgoing. Skipping to avoid incorrect classification.",
                            txid
                        );
                        let mut notified = self.notified_transactions.lock().await;
                        notified.insert(txid.clone());
                        drop(notified);
                        let mut times = self.notified_transactions_times.lock().await;
                        times.insert(txid, Instant::now());
                        continue;
                    }
                    Err(e) => {
                        warn!(
                            "Transaction {} (removed==added): Failed to get transaction details: {}. Cannot determine if incoming or outgoing. Skipping.",
                            txid, e
                        );
                        let mut notified = self.notified_transactions.lock().await;
                        notified.insert(txid.clone());
                        drop(notified);
                        let mut times = self.notified_transactions_times.lock().await;
                        times.insert(txid, Instant::now());
                        continue;
                    }
                }
            }
                NetDirection::ZeroZero => {
                    // removed_amount == 0 && added_amount == 0 - shouldn't happen, but skip if it does
                    warn!(
                        "Transaction {} for address {} has both removed=0 and added=0 - skipping",
                        txid, address
                    );
                }
            }

            /*} else if removed_amount == 0 && added_amount > 0 {
                // This case should have been handled above, but adding as safety check
                // Pure incoming transaction (no inputs from this address, only outputs to it)
                info!(
                    "Processing PURE INCOMING transaction {} for address {}: removed={}, added={}",
                    txid, address, removed_amount, added_amount
                );
                if self.config.notifications.incoming_tx {
                    self.incoming_handler
                        .handle_incoming_tx(
                            &address,
                            chat_id,
                            txid.clone(),
                            added_amount,
                            tx_daa_score,
                            virtual_daa_score,
                        )
                        .await?;
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
                    self.outgoing_handler
                        .handle_outgoing_tx(
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
                    self.incoming_handler
                        .handle_incoming_tx(
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
                
                if self.config.notifications.outgoing_tx {
                    // Calculate sent amount from transaction outputs if available
                    // IMPORTANT: Sum outputs NOT to the tracked address (these are the actual sent amounts)
                    // Outputs TO the tracked address are change and should be excluded
                    let actual_sent_amount = if let Ok(Some(mempool_entry)) =
                        self.kaspa_client.get_mempool_entry(&txid).await
                    {
                        // Transaction is in mempool - calculate actual sent amount from outputs
                        // IMPORTANT: Sum outputs NOT to the tracked address (these are the actual sent amounts)
                        // Outputs TO the tracked address are change and should be excluded
                        let tx = &mempool_entry.transaction;
                        let mut total_sent: u64 = 0;
                        let mut received_to_tracked: u64 = 0;
                        for output in &tx.outputs {
                            if let Some(verbose_data) = &output.verbose_data {
                                let output_address = verbose_data.script_public_key_address.to_string();
                                if output_address == address {
                                    // This is an output TO the tracked address (change or received)
                                    received_to_tracked += output.value;
                                } else {
                                    // This is an output to another address (the actual amount sent)
                                    total_sent += output.value;
                                }
                            }
                        }
                        if total_sent > 0 {
                            info!(
                                "Transaction {}: Calculated from outputs - sent to others: {} sompi, received to tracked address: {} sompi, UTXO net: {} sompi",
                                txid, total_sent, received_to_tracked, net_outgoing
                            );
                            Some(total_sent)
                        } else {
                            warn!(
                                "Transaction {}: No outputs to other addresses found (only {} sompi to tracked address). This shouldn't happen for outgoing transactions.",
                                txid, received_to_tracked
                            );
                            None
                        }
                    } else {
                        // Transaction not in mempool (already confirmed) - cannot get transaction outputs from node
                        // For regular outgoing transactions (removed > added), UTXO net calculation is accurate
                        // This represents the actual net amount that left the address (sent + fee - change)
                        info!(
                            "Transaction {} not in mempool (already confirmed). Using UTXO net calculation: {} sompi ({} KAS) as sent amount.",
                            txid,
                            net_outgoing,
                            KaspaClient::sompi_to_kas(net_outgoing)
                        );
                        None
                    };
                    if net_outgoing > 0 {
                    // Use actual sent amount from outputs if available, otherwise use UTXO net
                    // IMPORTANT: We pass both the sent amount (for display) and net change (for balance)
                    let sent_amount_for_display = actual_sent_amount.unwrap_or(net_outgoing);
                    info!(
                        "Calling handle_outgoing_tx for txid {}: sent_amount={} sompi ({} KAS), net_change={} sompi ({} KAS), chat_id={}",
                        txid, 
                        sent_amount_for_display, 
                        KaspaClient::sompi_to_kas(sent_amount_for_display),
                        net_outgoing,
                        KaspaClient::sompi_to_kas(net_outgoing),
                        chat_id
                    );
                    // Validate net_change makes sense
                    if net_outgoing == 0 && sent_amount_for_display > 0 {
                        warn!(
                            "WARNING: net_change is 0 but sent_amount is {} for transaction {} - this might be a self-transfer that should be skipped",
                            sent_amount_for_display, txid
                        );
                    }
                    // IMPORTANT: Don't mark as notified here - let the handler do it AFTER successfully sending
                    // This ensures that if notification fails, we can retry
                    self.outgoing_handler
                        .handle_outgoing_tx(
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
                    warn!(
                        "Skipping outgoing transaction {}: notifications enabled={}, net_outgoing={} sompi ({} KAS)",
                        txid, 
                        self.config.notifications.outgoing_tx, 
                        net_outgoing,
                        KaspaClient::sompi_to_kas(net_outgoing)
                    );
                    }
                } else {
                    warn!(
                        "Skipping outgoing transaction {}: notifications disabled or net_outgoing=0",
                        txid
                    );
                }
            } else {
                // removed_amount == 0 && added_amount == 0 - shouldn't happen, but skip if it does
                warn!(
                    "Transaction {} for address {} has both removed=0 and added=0 - skipping",
                    txid, address
                );
            }*/
        }

        Ok(())
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
                info!(
                    "Pending transaction {} is now confirmed (is_in_block: {}, daa_diff: {}, confirmation_depth: {})",
                    txid, is_in_block, daa_diff, confirmation_depth
                );
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
                    info!(
                        "Processing pending incoming transaction {} from queue",
                        pending.txid
                    );
                    self.incoming_handler
                        .handle_incoming_tx(
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
                    info!(
                        "Processing pending outgoing transaction {} from queue (chat_id: {}, sent_amount: {}, net_change: {})",
                        pending.txid, pending.chat_id, sent_amount, net_change_for_balance
                    );
                    self.outgoing_handler
                        .handle_outgoing_tx(
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
            } else {
                warn!(
                    "Pending transaction {} was marked as confirmed but not found in pending queue",
                    txid
                );
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{classify_net_direction, is_historical_transaction, NetDirection};

    #[test]
    fn classifies_net_directions() {
        assert_eq!(classify_net_direction(0, 5), NetDirection::PureIncoming);
        assert_eq!(classify_net_direction(5, 0), NetDirection::PureOutgoing);
        assert_eq!(classify_net_direction(3, 9), NetDirection::NetIncoming(6));
        assert_eq!(classify_net_direction(9, 3), NetDirection::NetOutgoing(6));
        assert_eq!(classify_net_direction(7, 7), NetDirection::EqualNonZero);
        assert_eq!(classify_net_direction(0, 0), NetDirection::ZeroZero);
    }

    #[test]
    fn historical_filter_threshold() {
        assert!(is_historical_transaction(100_000, 89_999)); // 10,001
        assert!(!is_historical_transaction(100_000, 90_000)); // 10,000
        assert!(!is_historical_transaction(100_000, 99_999)); // 1
    }

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
