use crate::kaspa_client::KaspaClient;
use crate::processor::NotificationProcessor;
use crate::telegram::TelegramClient;
use crate::config::WalletMode;
use crate::transaction_sender::{parse_kas_to_sompi, SendKaspaRequest, TransactionSender};
use anyhow::{Context, Result};
use kaspa_addresses::Address;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{info, warn};

pub struct CommandHandler {
    kaspa_client: Arc<KaspaClient>,
    telegram_client: Arc<TelegramClient>,
    transaction_sender: Arc<TransactionSender>,
    processor: Option<Arc<NotificationProcessor>>,
    // Rate limiting: track last command time per chat_id
    rate_limits: Arc<Mutex<HashMap<i64, Vec<Instant>>>>,
    // Multi-step /send flow state per user
    send_flows: Arc<Mutex<HashMap<i64, SendFlowState>>>,
}

#[derive(Debug, Clone)]
enum SendFlowState {
    AwaitingToAddress { from_address: String },
    AwaitingAmount { from_address: String, to_address: String },
}

impl CommandHandler {
    pub fn new(
        kaspa_client: Arc<KaspaClient>,
        telegram_client: Arc<TelegramClient>,
        transaction_sender: Arc<TransactionSender>,
    ) -> Self {
        Self {
            kaspa_client,
            telegram_client,
            transaction_sender,
            processor: None,
            rate_limits: Arc::new(Mutex::new(HashMap::new())),
            send_flows: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn set_processor(&mut self, processor: Arc<NotificationProcessor>) {
        self.processor = Some(processor);
    }

    // Check if user has exceeded rate limit
    // Returns true if rate limit exceeded, false otherwise
    async fn check_rate_limit(&self, chat_id: i64) -> bool {
        const MAX_COMMANDS: usize = 10;
        const WINDOW_SECONDS: u64 = 60;

        let mut limits = self.rate_limits.lock().await;
        let now = Instant::now();
        let window_start = now - Duration::from_secs(WINDOW_SECONDS);

        // Get or create rate limit history for this chat_id
        let commands = limits.entry(chat_id).or_insert_with(Vec::new);

        // Remove old commands outside the window
        commands.retain(|&time| time > window_start);

        // Check if limit exceeded
        if commands.len() >= MAX_COMMANDS {
            warn!(
                "Rate limit exceeded for chat_id: {} ({} commands in {}s)",
                chat_id,
                commands.len(),
                WINDOW_SECONDS
            );
            return true;
        }

        // Record this command
        commands.push(now);
        false
    }

    pub async fn handle_message(&self, chat_id: i64, text: &str) -> Result<()> {
        // Check rate limit (except for /help, /start, and /mining which are informational)
        let text_trimmed = text.trim();
        if !text_trimmed.starts_with("/help")
            && !text_trimmed.starts_with("/start")
            && !text_trimmed.starts_with("/mining")
            && self.check_rate_limit(chat_id).await
        {
            self.send_message(
                chat_id,
                "⏱️ <b>Rate Limit Exceeded</b>\n\nYou've sent too many commands. Please wait a minute before trying again.",
            )
            .await?;
            return Ok(());
        }

        let text = text_trimmed;

        if text.starts_with('/') {
            // Handle commands
            let parts: Vec<&str> = text.split_whitespace().collect();
            let command = parts[0];

            match command {
                "/cancel" => {
                    let removed = self.send_flows.lock().await.remove(&chat_id).is_some();
                    if removed {
                        self.send_message(chat_id, "✅ Active send flow cancelled.").await?;
                    } else {
                        self.send_message(chat_id, "ℹ️ No active send flow to cancel.")
                            .await?;
                    }
                }
                "/start" | "/help" => {
                    self.send_help(chat_id).await?;
                }
                "/add" => {
                    if parts.len() < 2 {
                        self.send_message(
                            chat_id,
                            "❌ <b>Usage:</b> <code>/add kaspa:qpxxxxxx...</code>\n\nPlease provide a Kaspa address to track.",
                        )
                        .await?;
                        return Ok(());
                    }
                    let address = parts[1];
                    self.handle_add_address(chat_id, address).await?;
                }
                "/remove" => {
                    if parts.len() < 2 {
                        self.send_message(
                            chat_id,
                            "❌ <b>Usage:</b> <code>/remove kaspa:qpxxxxxx...</code>\n\nPlease provide a Kaspa address to stop tracking.",
                        )
                        .await?;
                        return Ok(());
                    }
                    let address = parts[1];
                    self.handle_remove_address(chat_id, address).await?;
                }
                "/list" => {
                    self.handle_list_addresses(chat_id).await?;
                }
                "/refresh" => {
                    self.handle_refresh_balances(chat_id).await?;
                }
                "/mining" => {
                    self.handle_mining(chat_id).await?;
                }
                "/balance" => {
                    self.handle_balance(chat_id).await?;
                }
                "/rewards" => {
                    self.handle_rewards(chat_id).await?;
                }
                "/send" => {
                    self.handle_send(chat_id, &parts).await?;
                }
                "/wallet" => {
                    self.handle_wallet(chat_id, &parts).await?;
                }
                _ => {
                    self.send_message(
                        chat_id,
                        "❌ Unknown command. Use /help to see available commands.",
                    )
                    .await?;
                }
            }
        } else {
            // Continue /send flow if active
            if self.try_handle_send_flow_step(chat_id, text).await? {
                return Ok(());
            }

            // Try to parse as an address
            if Address::try_from(text).is_ok() {
                // It's a valid address, add it
                self.handle_add_address(chat_id, text).await?;
            } else {
                self.send_message(
                    chat_id,
                    "❌ Invalid command or address.\n\nSend a Kaspa address to track it, or use /help for commands.",
                )
                .await?;
            }
        }

        Ok(())
    }

    async fn send_help(&self, chat_id: i64) -> Result<()> {
        let help_text = r#"
🤖 <b>Kaspa Telegram Bot Commands</b>

<b>Commands:</b>
• <code>/add &lt;address&gt;</code> - Add a Kaspa address to track
• <code>/remove &lt;address&gt;</code> - Remove a tracked address
• <code>/list</code> - List all tracked addresses
• <code>/balance</code> - Check wallet balances
• <code>/rewards</code> - View recent block rewards
• <code>/refresh</code> - Refresh balances from blockchain
• <code>/send &lt;from&gt; &lt;to&gt; &lt;amount&gt;</code> - Send KAS from your wallet key
• <code>/cancel</code> - Cancel active send flow
• <code>/wallet status</code> - Show wallet credential state
• <code>/wallet balance</code> - Show balance for your imported wallet
• <code>/wallet importpk &lt;hex&gt;</code> - Import your private key for this bot session
• <code>/wallet clear</code> - Remove your session wallet credential
• <code>/mining</code> - Show mining pool connection details
• <code>/help</code> - Show this help message

<b>Quick Add:</b>
Just send a Kaspa address (e.g., <code>kaspa:qpxxxxxx...</code>) to automatically start tracking it.

<b>Example:</b>
<code>/add kaspa:qpxxxxxx...</code>
or simply:
<code>kaspa:qpxxxxxx...</code>

<b>Send Example:</b>
<code>/send</code> then follow prompts
"#;
        self.send_message(chat_id, help_text).await
    }

    async fn handle_wallet(&self, chat_id: i64, parts: &[&str]) -> Result<()> {
        if parts.len() < 2 {
            self.send_message(
                chat_id,
                "🔐 <b>Wallet Commands</b>\n\n<code>/wallet status</code>\n<code>/wallet balance</code>\n<code>/wallet importpk &lt;64-hex-private-key&gt;</code>\n<code>/wallet clear</code>",
            )
            .await?;
            return Ok(());
        }

        match parts[1] {
            "status" => {
                let mode_label = match self.transaction_sender.mode() {
                    WalletMode::Watchonly => "watchonly",
                    WalletMode::Local => "local",
                    WalletMode::External => "external",
                };
                let enabled = self.transaction_sender.is_enabled().await;
                let has_credential = self.transaction_sender.has_user_private_key(chat_id).await;
                let credential_mode = if self.transaction_sender.allow_user_credentials() {
                    "enabled"
                } else {
                    "disabled"
                };
                let msg = format!(
                    "🔐 <b>Wallet Status</b>\n\n\
                    Mode: <code>{}</code>\n\
                    Send enabled: <code>{}</code>\n\
                    User credential commands: <code>{}</code>\n\
                    Your credential loaded: <code>{}</code>",
                    mode_label, enabled, credential_mode, has_credential
                );
                self.send_message(chat_id, &msg).await?;
            }
            "balance" => {
                if !self.transaction_sender.has_user_private_key(chat_id).await {
                    self.send_message(
                        chat_id,
                        "🔑 No wallet credential loaded for your user.\n\nUse <code>/wallet importpk &lt;64-hex-private-key&gt;</code> first.",
                    )
                    .await?;
                    return Ok(());
                }

                let wallet_address = match self.transaction_sender.get_user_wallet_address(chat_id).await {
                    Ok(addr) => addr,
                    Err(e) => {
                        self.send_message(
                            chat_id,
                            &format!("❌ Failed to resolve your wallet address: {}", e),
                        )
                        .await?;
                        return Ok(());
                    }
                };

                let balance_sompi = match self.kaspa_client.get_balance(&wallet_address).await {
                    Ok(v) => v,
                    Err(e) => {
                        self.send_message(
                            chat_id,
                            &format!("❌ Failed to fetch wallet balance: {}", e),
                        )
                        .await?;
                        return Ok(());
                    }
                };

                let balance_kas = KaspaClient::sompi_to_kas(balance_sompi);
                self.send_message(
                    chat_id,
                    &format!(
                        "💰 <b>Wallet Balance</b>\n\n<b>Address:</b> <code>{}</code>\n<b>Balance:</b> <b>{:.8} KAS</b>",
                        wallet_address, balance_kas
                    ),
                )
                .await?;
            }
            "importpk" => {
                if parts.len() < 3 {
                    self.send_message(
                        chat_id,
                        "❌ <b>Usage:</b> <code>/wallet importpk &lt;64-hex-private-key&gt;</code>",
                    )
                    .await?;
                    return Ok(());
                }

                match self
                    .transaction_sender
                    .set_user_private_key(chat_id, parts[2])
                    .await
                {
                    Ok(_) => {
                        self.send_message(
                            chat_id,
                            "✅ Wallet credential loaded for this session.\n\nUse <code>/send</code> to create and broadcast transactions.",
                        )
                        .await?;
                    }
                    Err(e) => {
                        self.send_message(
                            chat_id,
                            &format!("❌ Failed to import credential: {}", e),
                        )
                        .await?;
                    }
                }
            }
            "clear" => {
                let cleared = self.transaction_sender.clear_user_private_key(chat_id).await;
                if cleared {
                    self.send_message(chat_id, "✅ Wallet credential removed from this session.")
                        .await?;
                } else {
                    self.send_message(chat_id, "ℹ️ No wallet credential was loaded for your chat.")
                        .await?;
                }
            }
            _ => {
                self.send_message(
                    chat_id,
                    "❌ Unknown wallet subcommand. Use <code>/wallet status</code>, <code>/wallet balance</code>, <code>/wallet importpk</code>, or <code>/wallet clear</code>.",
                )
                .await?;
            }
        }

        Ok(())
    }

    async fn handle_send(&self, chat_id: i64, parts: &[&str]) -> Result<()> {
        if !self.transaction_sender.is_enabled().await {
            self.send_message(
                chat_id,
                "🔒 <b>Send is disabled</b>\n\nBot operator has disabled send mode in configuration.",
            )
            .await?;
            return Ok(());
        }

        if parts.len() > 1 {
            self.send_message(
                chat_id,
                "ℹ️ <b>Send Wizard</b>\n\nJust use <code>/send</code> without parameters and follow prompts.\nUse <code>/cancel</code> to abort.",
            )
            .await?;
            return Ok(());
        }

        if !self.transaction_sender.has_user_private_key(chat_id).await {
            self.send_message(
                chat_id,
                "🔑 No wallet credential loaded for your user.\n\nUse <code>/wallet importpk &lt;64-hex-private-key&gt;</code> first.",
            )
            .await?;
            return Ok(());
        }

        let from_address = match self.transaction_sender.get_user_wallet_address(chat_id).await {
            Ok(addr) => addr,
            Err(e) => {
                self.send_message(
                    chat_id,
                    &format!("❌ Unable to resolve your wallet address: {}", e),
                )
                .await?;
                return Ok(());
            }
        };

        self.send_flows.lock().await.insert(
            chat_id,
            SendFlowState::AwaitingToAddress {
                from_address: from_address.clone(),
            },
        );
        self.send_message(
            chat_id,
            &format!(
                "🧾 <b>Send Wizard Started</b>\n\n<b>From:</b> <code>{}</code>\n\nReply with destination Kaspa address.\nUse <code>/cancel</code> to abort.",
                from_address
            ),
        )
        .await?;

        Ok(())
    }

    async fn try_handle_send_flow_step(&self, chat_id: i64, text: &str) -> Result<bool> {
        let state = { self.send_flows.lock().await.get(&chat_id).cloned() };
        let Some(state) = state else {
            return Ok(false);
        };

        match state {
            SendFlowState::AwaitingToAddress { from_address } => {
                let to_address = match Address::try_from(text) {
                    Ok(addr) => addr.to_string(),
                    Err(e) => {
                        self.send_message(
                            chat_id,
                            &format!(
                                "❌ Invalid destination address: {}\n\nPlease enter a valid Kaspa address or <code>/cancel</code>.",
                                e
                            ),
                        )
                        .await?;
                        return Ok(true);
                    }
                };
                if to_address == from_address {
                    self.send_message(
                        chat_id,
                        "❌ Destination cannot be the same as source address.\n\nEnter another address or <code>/cancel</code>.",
                    )
                    .await?;
                    return Ok(true);
                }

                self.send_flows.lock().await.insert(
                    chat_id,
                    SendFlowState::AwaitingAmount {
                        from_address: from_address.clone(),
                        to_address: to_address.clone(),
                    },
                );
                self.send_message(
                    chat_id,
                    &format!(
                        "💸 <b>Destination Set</b>\n\n<b>From:</b> <code>{}</code>\n<b>To:</b> <code>{}</code>\n\nNow enter amount in KAS (example: <code>1.25</code>).",
                        from_address, to_address
                    ),
                )
                .await?;
                Ok(true)
            }
            SendFlowState::AwaitingAmount {
                from_address,
                to_address,
            } => {
                let amount_sompi = match parse_kas_to_sompi(text) {
                    Ok(v) => v,
                    Err(e) => {
                        self.send_message(
                            chat_id,
                            &format!(
                                "❌ Invalid amount: {}\n\nEnter amount in KAS (example: <code>1.25</code>) or <code>/cancel</code>.",
                                e
                            ),
                        )
                        .await?;
                        return Ok(true);
                    }
                };

                self.send_flows.lock().await.remove(&chat_id);
                self.send_message(chat_id, "🛰️ Preparing and submitting signed transaction...")
                    .await?;

                let result = self
                    .transaction_sender
                    .send_kaspa(SendKaspaRequest {
                        chat_id,
                        from_address: from_address.clone(),
                        to_address: to_address.clone(),
                        amount_sompi,
                    })
                    .await;

                match result {
                    Ok(sent) => {
                        let amount_kas = KaspaClient::sompi_to_kas(amount_sompi);
                        self.send_message(
                            chat_id,
                            &format!(
                                "✅ <b>Transaction Broadcasted</b>\n\n<b>From:</b> <code>{}</code>\n<b>To:</b> <code>{}</code>\n<b>Amount:</b> {:.8} KAS\n<b>TXID:</b> <a href=\"https://kaspa.stream/transactions/{}\">{}</a>",
                                from_address, to_address, amount_kas, sent.txid, sent.txid
                            ),
                        )
                        .await?;
                    }
                    Err(e) => {
                        warn!("Send transaction request failed for chat {}: {}", chat_id, e);
                        self.send_message(
                            chat_id,
                            "❌ Failed to sign/broadcast the transaction. Please verify wallet balance, destination address, and signer service health.",
                        )
                        .await?;
                    }
                }

                Ok(true)
            }
        }
    }

    async fn handle_add_address(&self, chat_id: i64, address: &str) -> Result<()> {
        // Validate address
        let addr = match Address::try_from(address) {
            Ok(addr) => addr,
            Err(e) => {
                self.send_message(
                    chat_id,
                    &format!(
                        "❌ <b>Invalid Kaspa address:</b> {}\n\nError: {}",
                        address, e
                    ),
                )
                .await?;
                return Ok(());
            }
        };

        let addr_str = addr.to_string();

        // Check if already tracked by this user
        let tracked = self.kaspa_client.get_user_addresses(chat_id);
        if tracked.contains(&addr_str) {
            self.send_message(
                chat_id,
                &format!(
                    "ℹ️ Address <code>{}</code> is already being tracked.",
                    addr_str
                ),
            )
            .await?;
            return Ok(());
        }

        // Check if address is tracked by another user
        if let Some(owner_chat_id) = self.kaspa_client.get_address_owner(&addr_str) {
            if owner_chat_id != chat_id {
                self.send_message(
                    chat_id,
                    &format!(
                        "ℹ️ Address <code>{}</code> is already being tracked by another user.",
                        addr_str
                    ),
                )
                .await?;
                return Ok(());
            }
        }

        // Add to tracking for this user
        self.kaspa_client
            .add_tracked_address(chat_id, addr_str.clone());

        // Get initial balance
        let balance = match self.kaspa_client.get_balance(&addr_str).await {
            Ok(bal) => bal,
            Err(e) => {
                warn!("Failed to get initial balance for {}: {}", addr_str, e);
                0
            }
        };

        let balance_kas = KaspaClient::sompi_to_kas(balance);

        info!(
            "Added tracked address: {} (balance: {} KAS)",
            addr_str, balance_kas
        );

        self.send_message(
            chat_id,
            &format!(
                "✅ <b>Address Added</b>\n\n\
                <b>Address:</b> <code>{}</code>\n\
                <b>Current Balance:</b> {:.8} KAS\n\n\
                You will now receive notifications for confirmed transactions to this address.",
                addr_str, balance_kas
            ),
        )
        .await?;

        Ok(())
    }

    async fn handle_remove_address(&self, chat_id: i64, address: &str) -> Result<()> {
        // Validate address format
        let addr = match Address::try_from(address) {
            Ok(addr) => addr,
            Err(e) => {
                self.send_message(
                    chat_id,
                    &format!(
                        "❌ <b>Invalid Kaspa address:</b> {}\n\nError: {}",
                        address, e
                    ),
                )
                .await?;
                return Ok(());
            }
        };

        let addr_str = addr.to_string();

        // Check if tracked by this user
        let tracked = self.kaspa_client.get_user_addresses(chat_id);
        if !tracked.contains(&addr_str) {
            self.send_message(
                chat_id,
                &format!(
                    "ℹ️ Address <code>{}</code> is not currently being tracked by you.",
                    addr_str
                ),
            )
            .await?;
            return Ok(());
        }

        // Remove from tracking for this user
        if self.kaspa_client.remove_tracked_address(chat_id, &addr_str) {
            info!("Removed tracked address: {}", addr_str);
            self.send_message(
                chat_id,
                &format!(
                    "✅ <b>Address Removed</b>\n\n\
                    Address <code>{}</code> is no longer being tracked.\n\n\
                    You will no longer receive notifications for this address.",
                    addr_str
                ),
            )
            .await?;
        } else {
            self.send_message(
                chat_id,
                &format!(
                    "ℹ️ Address <code>{}</code> was not being tracked.",
                    addr_str
                ),
            )
            .await?;
        }

        Ok(())
    }

    async fn handle_list_addresses(&self, chat_id: i64) -> Result<()> {
        let tracked = self.kaspa_client.get_user_addresses(chat_id);

        if tracked.is_empty() {
            self.send_message(
                chat_id,
                "📋 <b>Your Tracked Addresses</b>\n\nNo addresses are currently being tracked.\n\nUse <code>/add &lt;address&gt;</code> to add an address.",
            )
            .await?;
            return Ok(());
        }

        let mut message = format!("📋 <b>Your Tracked Addresses</b> ({})\n\n", tracked.len());

        for (i, addr) in tracked.iter().enumerate() {
            let balance = match self.kaspa_client.get_balance(addr).await {
                Ok(bal) => KaspaClient::sompi_to_kas(bal),
                Err(_) => 0.0,
            };
            message.push_str(&format!(
                "{}. <code>{}</code>\n   Balance: {:.8} KAS\n\n",
                i + 1,
                addr,
                balance
            ));
        }

        self.send_message(chat_id, &message).await?;

        Ok(())
    }

    async fn handle_refresh_balances(&self, chat_id: i64) -> Result<()> {
        let tracked = self.kaspa_client.get_user_addresses(chat_id);

        if tracked.is_empty() {
            self.send_message(
                chat_id,
                "ℹ️ No addresses to refresh. Add addresses with <code>/add</code> first.",
            )
            .await?;
            return Ok(());
        }

        self.send_message(chat_id, "🔄 Refreshing balances...")
            .await?;

        let mut updated = 0;
        let mut errors = 0;

        for addr in &tracked {
            match self.kaspa_client.get_balance(addr).await {
                Ok(_balance) => {
                    // Note: We don't update processor balances here as they're managed separately
                    // This is just for user information - balance fetched successfully
                    updated += 1;
                }
                Err(e) => {
                    warn!("Failed to refresh balance for {}: {}", addr, e);
                    errors += 1;
                }
            }
        }

        let message = if errors == 0 {
            format!(
                "✅ <b>Balances Refreshed</b>\n\nSuccessfully refreshed {} address(es).\n\nUse <code>/list</code> to see updated balances.",
                updated
            )
        } else {
            format!(
                "⚠️ <b>Balance Refresh</b>\n\nRefreshed {} address(es), {} error(s).\n\nUse <code>/list</code> to see balances.",
                updated, errors
            )
        };

        self.send_message(chat_id, &message).await?;
        Ok(())
    }

    async fn handle_mining(&self, chat_id: i64) -> Result<()> {
        let mining_info = r#"
🔹 <b>Kaspa Mining Dashboard:</b>

Mainnet: <a href="http://pool.rustykaspa.org:3030/">http://pool.rustykaspa.org:3030/</a>

TN10: <a href="http://pool.rustykaspa.org:3031/">http://pool.rustykaspa.org:3031/</a>

TN12: <a href="http://pool.rustykaspa.org:3032/">http://pool.rustykaspa.org:3032/</a>

👇 <b>Mainnet Connection Details (Fixed Difficulty)</b>

<code>stratum+tcp://pool.rustykaspa.org:5550</code> (Diff: 4)
<code>stratum+tcp://pool.rustykaspa.org:5551</code> (Diff: 32)
<code>stratum+tcp://pool.rustykaspa.org:5552</code> (Diff: 64)
<code>stratum+tcp://pool.rustykaspa.org:5553</code> (Diff: 128)
<code>stratum+tcp://pool.rustykaspa.org:5554</code> (Diff: 256)
<code>stratum+tcp://pool.rustykaspa.org:5555</code> (Diff: 512)
<code>stratum+tcp://pool.rustykaspa.org:5556</code> (Diff: 1024)
<code>stratum+tcp://pool.rustykaspa.org:5557</code> (Diff: 2048)
<code>stratum+tcp://pool.rustykaspa.org:5558</code> (Diff: 4096)
<code>stratum+tcp://pool.rustykaspa.org:5559</code> (Diff: 8192)
<code>stratum+tcp://pool.rustykaspa.org:5560</code> (Diff: 16384)

🧪 <b>Testnets (CPU - Fixed Diff: 1)</b>
<code>stratum+tcp://pool.rustykaspa.org:6555</code> (TN10)
<code>stratum+tcp://pool.rustykaspa.org:7555</code> (TN12)
"#;
        self.send_message(chat_id, mining_info).await?;
        Ok(())
    }

    async fn handle_balance(&self, chat_id: i64) -> Result<()> {
        let tracked = self.kaspa_client.get_user_addresses(chat_id);

        if tracked.is_empty() {
            self.send_message(
                chat_id,
                "💰 <b>Wallet Balances</b>\n\nNo addresses are currently being tracked.\n\nUse <code>/add &lt;address&gt;</code> to add an address.",
            )
            .await?;
            return Ok(());
        }

        let mut message = "💰 <b>Wallet Balances</b>\n\n".to_string();
        let mut total_balance = 0u64;

        for (i, addr) in tracked.iter().enumerate() {
            let balance = match self.kaspa_client.get_balance(addr).await {
                Ok(bal) => {
                    total_balance += bal;
                    KaspaClient::sompi_to_kas(bal)
                }
                Err(_) => 0.0,
            };
            message.push_str(&format!(
                "{}. <code>{}</code>\n   Balance: <b>{:.8} KAS</b>\n\n",
                i + 1,
                addr,
                balance
            ));
        }

        let total_kas = KaspaClient::sompi_to_kas(total_balance);
        message.push_str(&format!("<b>Total Balance: {:.8} KAS</b>", total_kas));

        self.send_message(chat_id, &message).await?;
        Ok(())
    }

    async fn handle_rewards(&self, chat_id: i64) -> Result<()> {
        if let Some(processor) = &self.processor {
            let rewards = processor.get_block_rewards_history(chat_id).await;

            if rewards.is_empty() {
                self.send_message(
                    chat_id,
                    "⛏️ <b>Block Rewards History</b>\n\nNo block rewards found yet.\n\nBlock rewards will appear here once you receive them.",
                )
                .await?;
                return Ok(());
            }

            let mut message = "⛏️ <b>Block Rewards History</b>\n\n".to_string();
            message.push_str(&format!(
                "Found <b>{} block reward(s)</b>\n\n",
                rewards.len()
            ));

            // Sort rewards in descending chronological order (newest first)
            // This ensures balance progression is logical (newer rewards show higher balances)
            let mut sorted_rewards = rewards.clone();
            sorted_rewards.sort_by(|a, b| {
                // Sort by DAA score (newest first) as primary, then timestamp as tiebreaker
                // DAA score is more reliable for chronological ordering in blockchain contexts
                b.block_daa_score
                    .cmp(&a.block_daa_score)
                    .then_with(|| b.timestamp.cmp(&a.timestamp))
            });

            // Show rewards in descending chronological order (newest first)
            for (i, reward) in sorted_rewards.iter().take(20).enumerate() {
                let timestamp_seconds = if reward.timestamp > 1_000_000_000_000 {
                    reward.timestamp / 1000
                } else {
                    reward.timestamp
                };

                let timestamp_str =
                    chrono::DateTime::<chrono::Utc>::from_timestamp(timestamp_seconds as i64, 0)
                        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                        .unwrap_or_else(|| "Unknown".to_string());

                let block_url = format!("https://kaspa.stream/blocks/{}", reward.block_hash);
                message.push_str(&format!(
                    "{}. <b>+{:.8} KAS</b>\n   Address: <code>{}</code>\n   🔴 Previous: {:.8} KAS → 🟢 New: {:.8} KAS\n   Block: <a href=\"{}\">{}</a>\n   📊 DAA: {}\n   🕐 Time: {}\n\n",
                    i + 1,
                    reward.reward,
                    reward.address,
                    reward.previous_balance,
                    reward.new_balance,
                    block_url,
                    &reward.block_hash[..16],
                    reward.block_daa_score,
                    timestamp_str
                ));
            }

            if sorted_rewards.len() > 20 {
                message.push_str(&format!(
                    "... and {} more reward(s)",
                    sorted_rewards.len() - 20
                ));
            }

            self.send_message(chat_id, &message).await?;
        } else {
            self.send_message(chat_id, "❌ Block rewards history is not available.")
                .await?;
        }
        Ok(())
    }

    async fn send_message(&self, chat_id: i64, text: &str) -> Result<()> {
        self.telegram_client
            .send_message_to_chat(&chat_id.to_string(), text)
            .await
            .context("Failed to send message")
    }
}
