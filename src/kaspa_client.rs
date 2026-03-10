use anyhow::{Context, Result};
use kaspa_addresses::Address;
use kaspa_grpc_client::GrpcClient;
use kaspa_notify::listener::ListenerId;
use kaspa_rpc_core::api::rpc::RpcApi;
use kaspa_rpc_core::notify::mode::NotificationMode;
use kaspa_rpc_core::{
    GetBlockDagInfoRequest, GetCurrentBlockColorRequest, GetUtxosByAddressesRequest, Notification,
    RpcHash, RpcTransactionId,
};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, info, warn};

const KAS_TO_SOMPI: f64 = 100_000_000.0;

fn normalize_grpc_address(node_address: &str) -> String {
    if node_address.starts_with("grpc://") {
        node_address.to_string()
    } else {
        format!("grpc://{}", node_address)
    }
}

fn is_mempool_not_found_error(err: &str) -> bool {
    err.to_lowercase().contains("not found")
}

pub struct KaspaClient {
    client: Arc<GrpcClient>,
    notification_rx: Arc<Mutex<Option<mpsc::UnboundedReceiver<Notification>>>>,
    // Maps user chat_id to their tracked addresses
    user_addresses: Arc<parking_lot::Mutex<HashMap<i64, HashSet<String>>>>,
    // Maps address to subscribed chat_ids for quick fan-out lookup
    address_to_users: Arc<parking_lot::Mutex<HashMap<String, HashSet<i64>>>>,
    // Cache of recent transactions from blocks (txid -> transaction data)
    // This allows us to look up transaction details even after they're confirmed
    recent_transactions: Arc<tokio::sync::Mutex<HashMap<String, kaspa_rpc_core::RpcTransaction>>>,
}

impl KaspaClient {
    pub async fn new(node_address: String) -> Result<Self> {
        info!("Connecting to Kaspa node at {}", node_address);

        let grpc_address = normalize_grpc_address(&node_address);

        let mut attempt = 0u64;
        let mut backoff_ms = 250u64;

        let client = loop {
            attempt += 1;
            let connect_fut = GrpcClient::connect_with_args(
                NotificationMode::Direct,
                grpc_address.clone(),
                None,
                true,
                None,
                false,
                Some(500_000),
                Default::default(),
            );

            match connect_fut.await {
                Ok(client) => break Arc::new(client),
                Err(e) => {
                    let backoff = std::time::Duration::from_millis(backoff_ms);
                    warn!(
                        "Failed to connect to Kaspa node (attempt {}): {}, retrying in {:.2}s",
                        attempt,
                        e,
                        backoff.as_secs_f64()
                    );
                    tokio::time::sleep(backoff).await;
                    backoff_ms = (backoff_ms.saturating_mul(2)).min(5_000);
                }
            }
        };

        info!("Successfully connected to Kaspa node");

        client.start(None).await;

        // Subscribe to block added notifications
        let mut attempt = 0u64;
        let mut backoff_ms = 250u64;
        loop {
            attempt += 1;
            match client
                .start_notify(
                    ListenerId::default(),
                    kaspa_notify::scope::BlockAddedScope {}.into(),
                )
                .await
            {
                Ok(_) => break,
                Err(e) => {
                    let backoff = std::time::Duration::from_millis(backoff_ms);
                    warn!(
                        "Failed to subscribe to block notifications (attempt {}): {}, retrying in {:.2}s",
                        attempt,
                        e,
                        backoff.as_secs_f64()
                    );
                    tokio::time::sleep(backoff).await;
                    backoff_ms = (backoff_ms.saturating_mul(2)).min(5_000);
                }
            }
        }

        // Subscribe to UTXO changed notifications
        let mut attempt = 0u64;
        let mut backoff_ms = 250u64;
        loop {
            attempt += 1;
            match client
                .start_notify(
                    ListenerId::default(),
                    kaspa_notify::scope::UtxosChangedScope {
                        addresses: vec![], // Empty means all addresses
                    }
                    .into(),
                )
                .await
            {
                Ok(_) => break,
                Err(e) => {
                    let backoff = std::time::Duration::from_millis(backoff_ms);
                    warn!(
                        "Failed to subscribe to UTXO notifications (attempt {}): {}, retrying in {:.2}s",
                        attempt,
                        e,
                        backoff.as_secs_f64()
                    );
                    tokio::time::sleep(backoff).await;
                    backoff_ms = (backoff_ms.saturating_mul(2)).min(5_000);
                }
            }
        }

        // Set up notification receiver
        let receiver = client.notification_channel_receiver();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let receiver_clone = receiver.clone();
        tokio::spawn(async move {
            while let Ok(notification) = receiver_clone.recv().await {
                let _ = tx.send(notification);
            }
        });

        info!("Subscribed to Kaspa notifications");

        Ok(Self {
            client,
            notification_rx: Arc::new(Mutex::new(Some(rx))),
            user_addresses: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            address_to_users: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            recent_transactions: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        })
    }

    // Add an address to track for a specific user
    pub fn add_tracked_address(&self, chat_id: i64, address: String) {
        let mut user_addrs = self.user_addresses.lock();
        let mut addr_to_users = self.address_to_users.lock();

        user_addrs
            .entry(chat_id)
            .or_default()
            .insert(address.clone());
        addr_to_users.entry(address).or_default().insert(chat_id);
    }

    // Remove an address from tracking for a specific user
    pub fn remove_tracked_address(&self, chat_id: i64, address: &str) -> bool {
        let mut user_addrs = self.user_addresses.lock();
        let mut addr_to_users = self.address_to_users.lock();

        // Remove from user's address set
        if let Some(addrs) = user_addrs.get_mut(&chat_id) {
            if !addrs.remove(address) {
                return false;
            }
            if addrs.is_empty() {
                user_addrs.remove(&chat_id);
            }
        } else {
            return false;
        }

        // Remove this user from address subscriber set
        if let Some(subscribers) = addr_to_users.get_mut(address) {
            subscribers.remove(&chat_id);
            if subscribers.is_empty() {
                addr_to_users.remove(address);
            }
        }

        true
    }

    // Get all addresses tracked by a specific user
    pub fn get_user_addresses(&self, chat_id: i64) -> Vec<String> {
        self.user_addresses
            .lock()
            .get(&chat_id)
            .map(|addrs| addrs.iter().cloned().collect())
            .unwrap_or_default()
    }

    // Get all tracked addresses (across all users) - for notification processing
    pub fn get_all_tracked_addresses(&self) -> Vec<String> {
        self.address_to_users.lock().keys().cloned().collect()
    }

    // Get all subscribers (chat_ids) for a specific address
    pub fn get_address_owners(&self, address: &str) -> Vec<i64> {
        let mut owners: Vec<i64> = self
            .address_to_users
            .lock()
            .get(address)
            .map(|ids| ids.iter().copied().collect())
            .unwrap_or_default();
        owners.sort_unstable();
        owners
    }

    pub async fn take_notification_receiver(
        &self,
    ) -> Option<mpsc::UnboundedReceiver<Notification>> {
        self.notification_rx.lock().await.take()
    }

    pub async fn get_virtual_daa_score(&self) -> Result<u64> {
        // Retry logic for transient errors
        let mut attempt = 0;
        let max_attempts = 3;
        let mut backoff_ms = 100;

        loop {
            attempt += 1;
            match self
                .client
                .get_block_dag_info_call(None, GetBlockDagInfoRequest {})
                .await
            {
                Ok(response) => return Ok(response.virtual_daa_score),
                Err(e) => {
                    if attempt >= max_attempts {
                        return Err(e).context("Failed to get block DAG info after retries");
                    }
                    warn!(
                        "Failed to get virtual DAA score (attempt {}/{}): {}, retrying in {}ms",
                        attempt, max_attempts, e, backoff_ms
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(1000);
                }
            }
        }
    }

    pub async fn get_mempool_entry(
        &self,
        txid: &str,
    ) -> Result<Option<kaspa_rpc_core::RpcMempoolEntry>> {
        let txid_hash = RpcHash::from_str(txid)
            .with_context(|| format!("Failed to parse transaction ID: {}", txid))?;
        let txid = RpcTransactionId::from(txid_hash);

        // Retry logic for transient errors
        let mut attempt = 0;
        let max_attempts = 3;
        let mut backoff_ms = 100;

        loop {
            attempt += 1;
            match self
                .client
                .get_mempool_entry_call(
                    None,
                    kaspa_rpc_core::GetMempoolEntryRequest {
                        transaction_id: txid,
                        filter_transaction_pool: false, // false = include all transactions
                        include_orphan_pool: false,
                    },
                )
                .await
            {
                Ok(response) => return Ok(Some(response.mempool_entry)),
                Err(e) => {
                    // Check if error is "transaction not found" (expected for confirmed transactions)
                    let is_not_found = is_mempool_not_found_error(&e.to_string());

                    // If the node says it's not found in the mempool, treat it as confirmed and
                    // return immediately. Retrying here just adds latency to notifications.
                    if is_not_found {
                        debug!(
                            "Transaction {} not in mempool (likely confirmed): {}",
                            txid, e
                        );
                        return Ok(None);
                    }

                    if attempt >= max_attempts {
                        // Transaction might not be in mempool (already confirmed)
                        debug!(
                            "Transaction {} not found in mempool after {} attempts: {}",
                            txid, max_attempts, e
                        );
                        return Ok(None);
                    }
                    // Transient error, retry with backoff
                    // Only warn if it's not a "not found" error (which is expected for confirmed transactions)
                    warn!(
                        "Failed to get mempool entry (attempt {}/{}): {}, retrying in {}ms",
                        attempt, max_attempts, e, backoff_ms
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(1000);
                }
            }
        }
    }

    // Calculate transaction fee from transaction data
    // Returns fee in sompi, or None if calculation is not possible
    // Note: Input amounts are not directly available in the transaction structure.
    // To calculate fees accurately, we would need to look up each UTXO referenced
    // by the inputs, which requires additional RPC calls. For now, this function
    // returns None and the processor uses a default estimate.
    pub async fn calculate_transaction_fee(
        &self,
        _tx: &kaspa_rpc_core::RpcTransaction,
    ) -> Option<u64> {
        // Input amounts are not available in RpcTransactionInputVerboseData.
        // To calculate fees properly, we would need to:
        // 1. For each input, get the previous_outpoint
        // 2. Look up the UTXO using GetUtxosByAddresses or similar
        // 3. Sum all input UTXO amounts
        // 4. Sum all output amounts
        // 5. Fee = inputs - outputs
        //
        // This would require multiple RPC calls per transaction, which is expensive.
        // For now, return None and let the processor use a default estimate.
        None
    }

    pub async fn get_balance(&self, address: &str) -> Result<u64> {
        let addr = Address::try_from(address)
            .with_context(|| format!("Failed to parse address: {}", address))?;

        // Retry logic for transient errors
        let mut attempt = 0;
        let max_attempts = 3;
        let mut backoff_ms = 100;

        loop {
            attempt += 1;
            match self
                .client
                .get_utxos_by_addresses_call(
                    None,
                    GetUtxosByAddressesRequest::new(vec![addr.clone()]),
                )
                .await
            {
                Ok(response) => {
                    let balance: u64 = response
                        .entries
                        .iter()
                        .map(|entry| entry.utxo_entry.amount)
                        .sum();
                    return Ok(balance);
                }
                Err(e) => {
                    if attempt >= max_attempts {
                        return Err(e).with_context(|| {
                            format!("Failed to get UTXOs for {} after retries", address)
                        });
                    }
                    warn!(
                        "Failed to get balance for {} (attempt {}/{}): {}, retrying in {}ms",
                        address, attempt, max_attempts, e, backoff_ms
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(1000);
                }
            }
        }
    }

    pub async fn has_utxo_outpoint(
        &self,
        address: &str,
        txid: &str,
        output_index: u32,
        expected_amount: u64,
    ) -> Result<bool> {
        let addr = Address::try_from(address)
            .with_context(|| format!("Failed to parse address: {}", address))?;
        let txid_hash = RpcHash::from_str(txid)
            .with_context(|| format!("Failed to parse transaction ID: {}", txid))?;
        let target_txid = RpcTransactionId::from(txid_hash);

        let response = self
            .client
            .get_utxos_by_addresses_call(None, GetUtxosByAddressesRequest::new(vec![addr]))
            .await
            .with_context(|| format!("Failed to get UTXOs for {}", address))?;

        for entry in response.entries {
            if entry.outpoint.transaction_id == target_txid
                && entry.outpoint.index == output_index
                && entry.utxo_entry.amount == expected_amount
            {
                return Ok(true);
            }
        }

        Ok(false)
    }

    // Check if a block is blue confirmed (in the selected chain)
    // Uses the same API as the bridge code: get_current_block_color_call
    pub async fn is_block_blue_confirmed(&self, block_hash: &str) -> Result<bool> {
        let block_hash_rpc = RpcHash::from_str(block_hash)
            .with_context(|| format!("Failed to parse block hash: {}", block_hash))?;

        // Retry logic for transient errors (matching bridge code approach)
        let mut attempt = 0;
        let max_attempts = 3;
        let mut backoff_ms = 100;

        loop {
            attempt += 1;
            match self
                .client
                .get_current_block_color_call(
                    None,
                    GetCurrentBlockColorRequest {
                        hash: block_hash_rpc,
                    },
                )
                .await
            {
                Ok(response) => {
                    // Directly return the blue status from the API
                    // This matches the bridge code's approach in share_handler.rs:829
                    return Ok(response.blue);
                }
                Err(e) => {
                    // Check if error is "doesn't have any merger block" - this is expected for new blocks
                    let error_msg = e.to_string();
                    let is_no_merger_block = error_msg.contains("doesn't have any merger block");

                    if attempt >= max_attempts {
                        // If block not found or query fails, assume not blue
                        // For "no merger block" errors, this is expected and should be debug level
                        if is_no_merger_block {
                            debug!(
                                "Block {} doesn't have merger block yet (expected for new blocks): {}",
                                block_hash, e
                            );
                        } else {
                            debug!(
                                "Failed to get block color for {} after {} attempts: {}",
                                block_hash, max_attempts, e
                            );
                        }
                        return Ok(false);
                    }

                    // Only warn on retries if it's not a "no merger block" error
                    // "No merger block" is expected for new blocks and shouldn't spam logs
                    if is_no_merger_block {
                        debug!(
                            "Block {} doesn't have merger block yet (attempt {}/{}), retrying in {}ms",
                            block_hash, attempt, max_attempts, backoff_ms
                        );
                    } else {
                        warn!(
                            "Failed to get block color for {} (attempt {}/{}): {}, retrying in {}ms",
                            block_hash, attempt, max_attempts, e, backoff_ms
                        );
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(1000);
                }
            }
        }
    }

    pub fn sompi_to_kas(sompi: u64) -> f64 {
        sompi as f64 / KAS_TO_SOMPI
    }

    // Store transaction from block for later lookup
    pub async fn store_transaction_from_block(&self, tx: &kaspa_rpc_core::RpcTransaction) {
        if let Some(verbose_data) = &tx.verbose_data {
            let txid = verbose_data.transaction_id.to_string();
            let mut cache = self.recent_transactions.lock().await;
            // Keep only recent transactions (limit cache size to prevent memory issues)
            if cache.len() > 10000 {
                // Remove oldest 20% of entries
                let to_remove: Vec<String> = cache.keys().take(2000).cloned().collect();
                for key in to_remove {
                    cache.remove(&key);
                }
            }
            cache.insert(txid, tx.clone());
        }
    }

    // Get transaction amounts from mempool or block cache
    // Returns: (sent_amount, received_amount) where:
    // - sent_amount: sum of outputs NOT to the tracked address (for outgoing transactions)
    // - received_amount: sum of outputs TO the tracked address (for incoming transactions)
    pub async fn get_transaction_sent_amount(
        &self,
        txid: &str,
        tracked_address: &str,
    ) -> Result<Option<(u64, u64)>> {
        // First, try to get from mempool (with retry for recently confirmed transactions)
        let mut attempt = 0;
        let max_attempts = 3;
        let mut backoff_ms = 100;

        loop {
            attempt += 1;
            match self.get_mempool_entry(txid).await {
                Ok(Some(mempool_entry)) => {
                    // Transaction found in mempool - calculate from outputs
                    return self.calculate_sent_amount_from_tx(
                        &mempool_entry.transaction,
                        tracked_address,
                    );
                }
                Ok(None) if attempt < max_attempts => {
                    // Transaction not in mempool - might be very recently confirmed, retry once
                    debug!(
                        "Transaction {} not in mempool (attempt {}/{}), retrying in {}ms (may be very recently confirmed)",
                        txid, attempt, max_attempts, backoff_ms
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(500);
                    continue;
                }
                Ok(None) => {
                    // Transaction not in mempool - try block cache
                    break;
                }
                Err(e) => {
                    warn!("Failed to get mempool entry for {}: {}", txid, e);
                    // Try block cache as fallback
                    break;
                }
            }
        }

        // Try to get from block cache
        let cache = self.recent_transactions.lock().await;
        if let Some(tx) = cache.get(txid) {
            info!("Found transaction {} in block cache", txid);
            return self.calculate_sent_amount_from_tx(tx, tracked_address);
        }

        Ok(None)
    }

    pub async fn get_output_amount_to_address(
        &self,
        txid: &str,
        target_address: &str,
    ) -> Result<Option<u64>> {
        if let Ok(Some(mempool_entry)) = self.get_mempool_entry(txid).await {
            let amount = mempool_entry
                .transaction
                .outputs
                .iter()
                .filter_map(|o| {
                    o.verbose_data
                        .as_ref()
                        .map(|v| (v.script_public_key_address.to_string(), o.value))
                })
                .filter(|(addr, _)| addr == target_address)
                .map(|(_, value)| value)
                .sum::<u64>();
            if amount > 0 {
                return Ok(Some(amount));
            }
        }

        let cache = self.recent_transactions.lock().await;
        if let Some(tx) = cache.get(txid) {
            let amount = tx
                .outputs
                .iter()
                .filter_map(|o| {
                    o.verbose_data
                        .as_ref()
                        .map(|v| (v.script_public_key_address.to_string(), o.value))
                })
                .filter(|(addr, _)| addr == target_address)
                .map(|(_, value)| value)
                .sum::<u64>();
            if amount > 0 {
                return Ok(Some(amount));
            }
        }

        Ok(None)
    }

    // Calculate amounts from transaction outputs
    // Returns: (sent_amount, received_amount) where:
    // - sent_amount: sum of outputs NOT to the tracked address
    // - received_amount: sum of outputs TO the tracked address
    fn calculate_sent_amount_from_tx(
        &self,
        tx: &kaspa_rpc_core::RpcTransaction,
        tracked_address: &str,
    ) -> Result<Option<(u64, u64)>> {
        let mut sent_amount: u64 = 0;
        let mut received_amount: u64 = 0;

        for output in &tx.outputs {
            if let Some(verbose_data) = &output.verbose_data {
                let output_address = verbose_data.script_public_key_address.to_string();
                if output_address == tracked_address {
                    // This is an output TO the tracked address (received/change)
                    received_amount += output.value;
                } else {
                    // This is an output to another address (the actual amount sent)
                    sent_amount += output.value;
                }
            }
        }

        if sent_amount > 0 || received_amount > 0 {
            Ok(Some((sent_amount, received_amount)))
        } else {
            Ok(None)
        }
    }

    // Check if a block is in the DAG's tip hashes
    // This is used to ensure blocks have propagated before checking blue status
    pub async fn is_block_in_tip_hashes(&self, block_hash: &str) -> Result<bool> {
        let block_hash_rpc = RpcHash::from_str(block_hash)
            .with_context(|| format!("Failed to parse block hash: {}", block_hash))?;

        match self
            .client
            .get_block_dag_info_call(None, GetBlockDagInfoRequest {})
            .await
        {
            Ok(response) => {
                let in_tips = response.tip_hashes.contains(&block_hash_rpc);
                Ok(in_tips)
            }
            Err(e) => {
                warn!("Failed to get block DAG info: {}", e);
                Ok(false)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{is_mempool_not_found_error, normalize_grpc_address, KAS_TO_SOMPI};

    #[test]
    fn normalizes_grpc_addresses() {
        assert_eq!(
            normalize_grpc_address("127.0.0.1:16110"),
            "grpc://127.0.0.1:16110"
        );
        assert_eq!(
            normalize_grpc_address("grpc://10.0.0.2:16110"),
            "grpc://10.0.0.2:16110"
        );
    }

    #[test]
    fn detects_mempool_not_found_errors_case_insensitive() {
        assert!(is_mempool_not_found_error("Transaction not found"));
        assert!(is_mempool_not_found_error("NOT FOUND in mempool"));
        assert!(!is_mempool_not_found_error("rpc timeout"));
    }

    #[test]
    fn kas_to_sompi_constant_is_expected() {
        assert_eq!(KAS_TO_SOMPI, 100_000_000.0);
    }
}
