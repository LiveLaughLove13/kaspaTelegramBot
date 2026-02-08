use crate::kaspa_client::KaspaClient;
use crate::processor::NotificationProcessor;
use crate::telegram::TelegramClient;
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
    processor: Option<Arc<NotificationProcessor>>,
    // Rate limiting: track last command time per chat_id
    rate_limits: Arc<Mutex<HashMap<i64, Vec<Instant>>>>,
}

impl CommandHandler {
    pub fn new(kaspa_client: Arc<KaspaClient>, telegram_client: Arc<TelegramClient>) -> Self {
        Self {
            kaspa_client,
            telegram_client,
            processor: None,
            rate_limits: Arc::new(Mutex::new(HashMap::new())),
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
                _ => {
                    self.send_message(
                        chat_id,
                        "❌ Unknown command. Use /help to see available commands.",
                    )
                    .await?;
                }
            }
        } else {
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
• <code>/mining</code> - Show mining pool connection details
• <code>/help</code> - Show this help message

<b>Quick Add:</b>
Just send a Kaspa address (e.g., <code>kaspa:qpxxxxxx...</code>) to automatically start tracking it.

<b>Example:</b>
<code>/add kaspa:qpxxxxxx...</code>
or simply:
<code>kaspa:qpxxxxxx...</code>
"#;
        self.send_message(chat_id, help_text).await
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
        message.push_str(&format!(
            "<b>Total Balance: {:.8} KAS</b>",
            total_kas
        ));

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

            // Show rewards in chronological order (oldest first)
            for (i, reward) in rewards.iter().take(20).enumerate() {
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

            if rewards.len() > 20 {
                message.push_str(&format!("... and {} more reward(s)", rewards.len() - 20));
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
