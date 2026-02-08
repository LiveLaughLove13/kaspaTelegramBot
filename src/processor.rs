use crate::block_processor::{BlockProcessor, BlockRewardRecord};
use crate::config::Config;
use crate::kaspa_client::KaspaClient;
use crate::telegram::TelegramClient;
use crate::transaction_processor::TransactionProcessor;
use anyhow::{Context, Result};
use kaspa_rpc_core::Notification;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, warn};

pub struct NotificationProcessor {
    // Fields used directly in NotificationProcessor methods
    kaspa_client: Arc<KaspaClient>,
    address_balances: Arc<Mutex<HashMap<String, u64>>>,
    block_rewards_history: Arc<Mutex<HashMap<i64, Vec<BlockRewardRecord>>>>,
    // Processors (contain their own copies of shared state)
    transaction_processor: TransactionProcessor,
    block_processor: BlockProcessor,
}

impl NotificationProcessor {
    pub fn new(
        kaspa_client: Arc<KaspaClient>,
        telegram_client: Arc<TelegramClient>,
        config: Arc<Config>,
    ) -> Self {
        // Initialize shared state
        let pending_transactions = Arc::new(Mutex::new(HashMap::new()));
        let pending_blocks = Arc::new(Mutex::new(HashMap::new()));
        let address_balances = Arc::new(Mutex::new(HashMap::new()));
        let notified_transactions = Arc::new(Mutex::new(HashSet::new()));
        let notified_blocks = Arc::new(Mutex::new(HashSet::new()));
        let notified_transactions_times = Arc::new(Mutex::new(HashMap::new()));
        let notified_blocks_times = Arc::new(Mutex::new(HashMap::new()));
        let block_rewards_history = Arc::new(Mutex::new(HashMap::new()));

        // Create processors with shared state
        let transaction_processor = TransactionProcessor::new(
            kaspa_client.clone(),
            telegram_client.clone(),
            config.clone(),
            pending_transactions.clone(),
            address_balances.clone(),
            notified_transactions.clone(),
            notified_transactions_times.clone(),
        );

        let block_processor = BlockProcessor::new(
            kaspa_client.clone(),
            telegram_client.clone(),
            config.clone(),
            pending_blocks.clone(),
            address_balances.clone(),
            notified_blocks.clone(),
            notified_blocks_times.clone(),
            block_rewards_history.clone(),
            notified_transactions.clone(),
            notified_transactions_times.clone(),
        );

        Self {
            kaspa_client,
            address_balances,
            block_rewards_history,
            transaction_processor,
            block_processor,
        }
    }

    pub async fn get_block_rewards_history(&self, chat_id: i64) -> Vec<BlockRewardRecord> {
        let history = self.block_rewards_history.lock().await;
        history.get(&chat_id).cloned().unwrap_or_default()
    }

    pub async fn initialize_balances(&self) -> Result<()> {
        let addresses = self.kaspa_client.get_all_tracked_addresses();
        let mut balances = self.address_balances.lock().await;

        for address in addresses {
            match self.kaspa_client.get_balance(&address).await {
                Ok(balance) => {
                    balances.insert(address.clone(), balance);
                    info!("Initialized balance for {}: {} sompi", address, balance);
                }
                Err(e) => {
                    warn!("Failed to get initial balance for {}: {}", address, e);
                }
            }
        }

        Ok(())
    }

    pub async fn process_notification(&self, notification: Notification) -> Result<()> {
        match notification {
            Notification::UtxosChanged(utxos_changed) => {
                self.transaction_processor
                    .handle_utxos_changed(utxos_changed)
                    .await?;
            }
            Notification::BlockAdded(block_added) => {
                self.block_processor.handle_block_added(block_added).await?;
            }
            _ => {
                tracing::debug!("Ignoring notification: {:?}", notification);
            }
        }
        Ok(())
    }

    pub async fn check_pending_confirmations(&self) -> Result<()> {
        let virtual_daa_score = self
            .kaspa_client
            .get_virtual_daa_score()
            .await
            .context("Failed to get virtual DAA score")?;

        // Check pending transactions
        self.transaction_processor
            .check_pending_confirmations(virtual_daa_score)
            .await?;

        // Check pending blocks
        self.block_processor
            .check_pending_confirmations(virtual_daa_score)
            .await?;

        Ok(())
    }

    // Refresh balances for all tracked addresses from the blockchain
    pub async fn refresh_balances(&self) -> Result<()> {
        let addresses = self.kaspa_client.get_all_tracked_addresses();
        let mut balances = self.address_balances.lock().await;

        for address in addresses {
            match self.kaspa_client.get_balance(&address).await {
                Ok(balance) => {
                    balances.insert(address.clone(), balance);
                }
                Err(e) => {
                    warn!("Failed to refresh balance for {}: {}", address, e);
                }
            }
        }

        Ok(())
    }
}
