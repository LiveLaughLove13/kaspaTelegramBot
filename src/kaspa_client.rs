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

pub struct KaspaClient {
    client: Arc<GrpcClient>,
    notification_rx: Arc<Mutex<Option<mpsc::UnboundedReceiver<Notification>>>>,
    // Maps user chat_id to their tracked addresses
    user_addresses: Arc<parking_lot::Mutex<HashMap<i64, HashSet<String>>>>,
    // Maps address to user chat_id for quick lookup
    address_to_user: Arc<parking_lot::Mutex<HashMap<String, i64>>>,
}

impl KaspaClient {
    pub async fn new(node_address: String) -> Result<Self> {
        info!("Connecting to Kaspa node at {}", node_address);

        let grpc_address = if node_address.starts_with("grpc://") {
            node_address.clone()
        } else {
            format!("grpc://{}", node_address)
        };

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
            address_to_user: Arc::new(parking_lot::Mutex::new(HashMap::new())),
        })
    }

    // Add an address to track for a specific user
    pub fn add_tracked_address(&self, chat_id: i64, address: String) {
        let mut user_addrs = self.user_addresses.lock();
        let mut addr_to_user = self.address_to_user.lock();

        user_addrs
            .entry(chat_id)
            .or_default()
            .insert(address.clone());
        addr_to_user.insert(address, chat_id);
    }

    // Remove an address from tracking for a specific user
    pub fn remove_tracked_address(&self, chat_id: i64, address: &str) -> bool {
        let mut user_addrs = self.user_addresses.lock();
        let mut addr_to_user = self.address_to_user.lock();

        // Check if this address belongs to this user
        if let Some(&owner_chat_id) = addr_to_user.get(address) {
            if owner_chat_id != chat_id {
                return false; // Address belongs to a different user
            }
        } else {
            return false; // Address not tracked
        }

        // Remove from user's address set
        if let Some(addrs) = user_addrs.get_mut(&chat_id) {
            addrs.remove(address);
            if addrs.is_empty() {
                user_addrs.remove(&chat_id);
            }
        }

        // Remove from address-to-user mapping
        addr_to_user.remove(address);
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
        self.address_to_user.lock().keys().cloned().collect()
    }

    // Get the user (chat_id) who owns a specific address
    pub fn get_address_owner(&self, address: &str) -> Option<i64> {
        self.address_to_user.lock().get(address).copied()
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
                    let is_not_found = e.to_string().to_lowercase().contains("not found");

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
