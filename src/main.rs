mod block_processor;
mod command_handler;
mod config;
mod kaspa_client;
mod processor;
mod telegram;
mod transaction_processor;

use anyhow::{Context, Result};
use command_handler::CommandHandler;
use config::Config;
use kaspa_client::KaspaClient;
use processor::NotificationProcessor;
use std::sync::Arc;
use telegram::TelegramClient;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    info!("Starting Kaspa Telegram Bot");

    // Load configuration
    info!("Loading config from: bot-config.toml");
    let config = Arc::new(Config::load().context("Failed to load config from bot-config.toml")?);

    // Validate configuration
    if config.telegram.bot_token.is_empty() {
        return Err(anyhow::anyhow!("Telegram bot token is required"));
    }

    // Create clients
    let kaspa_client = Arc::new(
        KaspaClient::new(config.kaspa.node_address.clone())
            .await
            .context("Failed to connect to Kaspa node")?,
    );

    // Create telegram client (chat_id not needed for multi-user mode)
    let telegram_client = Arc::new(TelegramClient::new(
        config.telegram.bot_token.clone(),
        String::new(), // Not used in multi-user mode
    ));

    // Create processor
    let processor = Arc::new(NotificationProcessor::new(
        kaspa_client.clone(),
        telegram_client.clone(),
        config.clone(),
    ));

    // Get tracked addresses from config or environment variable (for backward compatibility)
    // These will be associated with the configured chat_id if provided
    let wallet_addresses: Vec<String> = if !config.wallet_addresses.is_empty() {
        config.wallet_addresses.clone()
    } else {
        std::env::var("KASPA_WALLET_ADDRESSES")
            .ok()
            .map(|s| {
                s.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default()
    };

    // If addresses are configured and chat_id is provided, associate them with that user
    if !wallet_addresses.is_empty() && !config.telegram.chat_id.is_empty() {
        if let Ok(chat_id) = config.telegram.chat_id.parse::<i64>() {
            info!(
                "Tracking {} wallet addresses for configured chat_id",
                wallet_addresses.len()
            );
            for address in &wallet_addresses {
                kaspa_client.add_tracked_address(chat_id, address.clone());
                info!("Tracking address: {} for chat_id: {}", address, chat_id);
            }
        } else {
            warn!("Invalid chat_id in config, ignoring configured addresses");
        }
    } else if !wallet_addresses.is_empty() {
        warn!(
            "Addresses configured but no chat_id provided. Users must add addresses via commands."
        );
    }

    // Initialize balances
    processor
        .initialize_balances()
        .await
        .context("Failed to initialize balances")?;

    // Optional startup notification (only if chat_id is configured)
    if !config.telegram.chat_id.is_empty() {
        if let Ok(chat_id) = config.telegram.chat_id.parse::<i64>() {
            let startup_msg = format!(
                "🤖 <b>Kaspa Telegram Bot Started</b>\n\n\
                Multi-user mode enabled\n\
                Confirmation depth: {} DAA score\n\
                Node: {}",
                config.confirmation.daa_score_depth, config.kaspa.node_address
            );
            if let Err(e) = telegram_client
                .send_message_to_chat(&chat_id.to_string(), &startup_msg)
                .await
            {
                warn!("Failed to send startup notification: {}", e);
            }
        }
    }

    // Create command handler
    let mut command_handler = CommandHandler::new(kaspa_client.clone(), telegram_client.clone());
    command_handler.set_processor(processor.clone());
    let command_handler = Arc::new(command_handler);

    // Set bot commands menu (shows when user types /)
    if let Err(e) = telegram_client.set_commands().await {
        warn!("Failed to set bot commands menu: {}", e);
    } else {
        info!("Bot commands menu configured successfully");
    }

    // Start notification processing loop
    // Check frequently for low-latency notifications on a 10 BPS network
    let processor_clone = processor.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(250));
        loop {
            interval.tick().await;
            if let Err(e) = processor_clone.check_pending_confirmations().await {
                error!("Error checking pending confirmations: {}", e);
            }
        }
    });

    // Start periodic balance refresh (every hour)
    let processor_refresh = processor.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(300)); // 5 minutes
        loop {
            interval.tick().await;
            if let Err(e) = processor_refresh.refresh_balances().await {
                warn!("Error refreshing balances: {}", e);
            } else {
                info!("Periodically refreshed balances for all tracked addresses");
            }
        }
    });

    // Start Telegram command handler loop (polling for messages)
    let command_handler_clone = command_handler.clone();
    let polling_client = TelegramClient::new(config.telegram.bot_token.clone(), String::new());
    tokio::spawn(async move {
        let mut last_update_id: Option<i64> = None;
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));

        loop {
            interval.tick().await;

            match polling_client.get_updates(last_update_id).await {
                Ok(updates) => {
                    for update in updates {
                        last_update_id = Some(update.update_id + 1);

                        if let Some(message) = update.message {
                            if let Some(text) = message.text {
                                let chat_id = message.chat.id;
                                if let Err(e) =
                                    command_handler_clone.handle_message(chat_id, &text).await
                                {
                                    warn!("Error handling command: {}", e);
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("Error getting Telegram updates: {}", e);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            }
        }
    });

    // Main notification loop
    let mut notification_rx = kaspa_client
        .take_notification_receiver()
        .await
        .ok_or_else(|| anyhow::anyhow!("Notification receiver already taken"))?;

    loop {
        match notification_rx.recv().await {
            Some(notification) => {
                if let Err(e) = processor.process_notification(notification).await {
                    error!("Error processing notification: {}", e);
                }
            }
            None => {
                error!("Notification channel closed");
                break;
            }
        }
    }

    Ok(())
}
