mod ai;
mod block_processor;
mod command_handler;
mod config;
mod incoming_transaction;
mod kaspa_client;
mod outgoing_transaction;
mod processor;
mod telegram;
mod transaction_processor;
mod transaction_sender;

use ai::AiService;
use anyhow::{Context, Result};
use command_handler::CommandHandler;
use config::Config;
use kaspa_client::KaspaClient;
use processor::NotificationProcessor;
use reqwest::Url;
use std::sync::Arc;
use telegram::TelegramClient;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use transaction_sender::TransactionSender;

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

    // Auto-start local model runtime when configured for localhost endpoint.
    if let Err(e) = ensure_local_model_runtime(config.as_ref()).await {
        warn!("Local model runtime auto-start check failed: {}", e);
    }

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

    // Create transaction sender from bot-config.toml wallet settings
    let transaction_sender = Arc::new(TransactionSender::new(&config.wallet));
    let ai_service = Arc::new(AiService::new(&config.ai));
    if config.ai.enabled && config.ai.knowledge.enabled {
        let ai_startup_sync = ai_service.clone();
        tokio::spawn(async move {
            if let Err(e) = ai_startup_sync.sync_knowledge_repo().await {
                warn!("AI knowledge sync failed at startup: {}", e);
            } else {
                info!("AI knowledge startup sync completed");
            }
        });
    }
    if config.ai.enabled && config.ai.knowledge.enabled {
        let ai_refresh = ai_service.clone();
        let refresh_minutes = ai_refresh.knowledge_refresh_minutes();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_secs(refresh_minutes * 60));
            loop {
                interval.tick().await;
                if let Err(e) = ai_refresh.sync_knowledge_repo().await {
                    warn!("AI knowledge background refresh failed: {}", e);
                } else {
                    info!("AI knowledge repository refreshed");
                }
            }
        });
    }

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
    let mut command_handler = CommandHandler::new(
        kaspa_client.clone(),
        telegram_client.clone(),
        transaction_sender.clone(),
        ai_service.clone(),
    );
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

    // Start periodic balance refresh
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

async fn ensure_local_model_runtime(config: &Config) -> Result<()> {
    if !config.ai.enabled || !config.ai.local_model.enabled {
        return Ok(());
    }

    let endpoint = Url::parse(&config.ai.local_model.endpoint).with_context(|| {
        format!(
            "Invalid ai.local_model.endpoint: {}",
            config.ai.local_model.endpoint
        )
    })?;
    let host = endpoint.host_str().unwrap_or_default();
    let is_local = host.eq_ignore_ascii_case("127.0.0.1") || host.eq_ignore_ascii_case("localhost");
    if !is_local {
        return Ok(());
    }

    let base = format!(
        "{}://{}{}",
        endpoint.scheme(),
        host,
        endpoint
            .port()
            .map(|p| format!(":{}", p))
            .unwrap_or_default()
    );
    let tags_url = format!("{}/api/tags", base);

    if is_ollama_alive(&tags_url).await {
        info!("Local Ollama endpoint is already running at {}", base);
        return Ok(());
    }

    let cwd = std::env::current_dir().context("Failed to read current directory")?;
    let local_exe = cwd.join("tools").join("ollama").join("ollama.exe");

    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x08000000;
        let mut cmd = if local_exe.exists() {
            std::process::Command::new(local_exe)
        } else {
            std::process::Command::new("ollama")
        };
        cmd.arg("serve")
            .env("OLLAMA_HOST", &base)
            .creation_flags(CREATE_NO_WINDOW);
        let _child = cmd
            .spawn()
            .context("Failed to spawn local Ollama server process")?;
    }

    #[cfg(not(windows))]
    {
        let mut cmd = if local_exe.exists() {
            std::process::Command::new(local_exe)
        } else {
            std::process::Command::new("ollama")
        };
        cmd.arg("serve").env("OLLAMA_HOST", &base);
        let _child = cmd
            .spawn()
            .context("Failed to spawn local Ollama server process")?;
    }

    for _ in 0..20 {
        if is_ollama_alive(&tags_url).await {
            info!("Started local Ollama server at {}", base);
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    warn!(
        "Attempted to start local Ollama at {}, but health check did not pass yet",
        base
    );
    Ok(())
}

async fn is_ollama_alive(tags_url: &str) -> bool {
    reqwest::Client::new()
        .get(tags_url)
        .timeout(std::time::Duration::from_secs(2))
        .send()
        .await
        .map(|r| r.status().is_success())
        .unwrap_or(false)
}
