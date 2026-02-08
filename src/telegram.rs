use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;

pub struct TelegramClient {
    bot_token: String,
    client: reqwest::Client,
}

impl TelegramClient {
    pub fn new(bot_token: String, _chat_id: String) -> Self {
        // chat_id parameter kept for backward compatibility but not stored
        Self {
            bot_token,
            client: reqwest::Client::new(),
        }
    }

    // Set bot commands menu (shows when user types /)
    pub async fn set_commands(&self) -> Result<()> {
        let url = format!(
            "https://api.telegram.org/bot{}/setMyCommands",
            self.bot_token
        );

        let commands = json!([
            {"command": "start", "description": "Start the bot"},
            {"command": "help", "description": "Show help and available commands"},
            {"command": "add", "description": "Add a Kaspa address to track"},
            {"command": "remove", "description": "Remove a tracked address"},
            {"command": "list", "description": "List all tracked addresses"},
            {"command": "balance", "description": "Check wallet balances"},
            {"command": "rewards", "description": "View recent block rewards"},
            {"command": "refresh", "description": "Refresh balances from blockchain"},
            {"command": "mining", "description": "Show mining pool connection details"}
        ]);

        let payload = json!({
            "commands": commands
        });

        let response = self
            .client
            .post(&url)
            .json(&payload)
            .send()
            .await
            .context("Failed to set bot commands")?;

        let status = response.status();
        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Telegram API returned error: {} - {}",
                status,
                error_text
            ));
        }

        Ok(())
    }

    // Generate Kaspa.Stream URL for an address
    fn address_url(address: &str) -> String {
        format!("https://kaspa.stream/addresses/{}", address)
    }

    // Generate Kaspa.Stream URL for a transaction
    fn transaction_url(txid: &str) -> String {
        format!("https://kaspa.stream/transactions/{}", txid)
    }

    // Generate Kaspa.Stream URL for a block
    fn block_url(block_hash: &str) -> String {
        format!("https://kaspa.stream/blocks/{}", block_hash)
    }

    pub async fn send_message_to_chat(&self, chat_id: &str, text: &str) -> Result<()> {
        let url = format!("https://api.telegram.org/bot{}/sendMessage", self.bot_token);

        let payload = json!({
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": true,
        });

        let response = self
            .client
            .post(&url)
            .json(&payload)
            .send()
            .await
            .context("Failed to send HTTP request to Telegram API")?;

        let status = response.status();
        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Telegram API returned error: {} - {}",
                status,
                error_text
            ));
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn send_incoming_tx_to_chat(
        &self,
        chat_id: i64,
        address: &str,
        amount: f64,
        previous_balance: f64,
        new_balance: f64,
        txid: &str,
        daa_score: u64,
        timestamp: u64,
    ) -> Result<()> {
        // Timestamp is in milliseconds, convert to seconds for chrono
        let timestamp_seconds = if timestamp > 1_000_000_000_000 {
            // Timestamp is in milliseconds (13+ digits)
            timestamp / 1000
        } else {
            // Timestamp is already in seconds (10 digits)
            timestamp
        };

        let timestamp_str =
            chrono::DateTime::<chrono::Utc>::from_timestamp(timestamp_seconds as i64, 0)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                .unwrap_or_else(|| "Unknown".to_string());

        let address_url = Self::address_url(address);
        let tx_url = Self::transaction_url(txid);

        let message = format!(
            "📥 <b>Incoming Transaction Confirmed</b>\n\n\
            <b>Address:</b> <a href=\"{}\">{}</a>\n\
            💸 <b>Amount 📥 Received:</b> <b>+{:.8} KAS</b>\n\
            🔴 <b>Previous balance:</b> {:.8} KAS\n\
            🟢 <b>New balance:</b> {:.8} KAS\n\
            🔗 <b>TXID:</b> <a href=\"{}\">{}</a>\n\
            📊 <b>DAA Score:</b> {}\n\
            🕐 <b>Timestamp:</b> {}",
            address_url,
            address,
            amount,
            previous_balance,
            new_balance,
            tx_url,
            txid,
            daa_score,
            timestamp_str
        );

        self.send_message_to_chat(&chat_id.to_string(), &message)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn send_outgoing_tx_to_chat(
        &self,
        chat_id: i64,
        address: &str,
        amount: f64,
        fee: f64,
        previous_balance: f64,
        new_balance: f64,
        txid: &str,
        daa_score: u64,
        timestamp: u64,
    ) -> Result<()> {
        // Timestamp is in milliseconds, convert to seconds for chrono
        let timestamp_seconds = if timestamp > 1_000_000_000_000 {
            // Timestamp is in milliseconds (13+ digits)
            timestamp / 1000
        } else {
            // Timestamp is already in seconds (10 digits)
            timestamp
        };

        let timestamp_str =
            chrono::DateTime::<chrono::Utc>::from_timestamp(timestamp_seconds as i64, 0)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                .unwrap_or_else(|| "Unknown".to_string());

        let address_url = Self::address_url(address);
        let tx_url = Self::transaction_url(txid);

        let message = format!(
            "📤 <b>Outgoing Transaction Confirmed</b>\n\n\
            <b>Address:</b> <a href=\"{}\">{}</a>\n\
            💸 <b>Amount 📤 Sent:</b> <b>-{:.8} KAS</b>\n\
            💳 <b>Fee:</b> {:.8} KAS\n\
            🔴 <b>Previous balance:</b> {:.8} KAS\n\
            🟢 <b>New balance:</b> {:.8} KAS\n\
            🔗 <b>TXID:</b> <a href=\"{}\">{}</a>\n\
            📊 <b>DAA Score:</b> {}\n\
            🕐 <b>Timestamp:</b> {}",
            address_url,
            address,
            amount,
            fee,
            previous_balance,
            new_balance,
            tx_url,
            txid,
            daa_score,
            timestamp_str
        );

        self.send_message_to_chat(&chat_id.to_string(), &message)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn send_block_reward_to_chat(
        &self,
        chat_id: i64,
        address: &str,
        reward: f64,
        previous_balance: f64,
        new_balance: f64,
        block_hash: &str,
        block_daa_score: u64,
        timestamp: u64,
    ) -> Result<()> {
        // Timestamp is in milliseconds, convert to seconds for chrono
        let timestamp_seconds = if timestamp > 1_000_000_000_000 {
            // Timestamp is in milliseconds (13+ digits)
            timestamp / 1000
        } else {
            // Timestamp is already in seconds (10 digits)
            timestamp
        };

        let timestamp_str =
            chrono::DateTime::<chrono::Utc>::from_timestamp(timestamp_seconds as i64, 0)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                .unwrap_or_else(|| "Unknown".to_string());

        let address_url = Self::address_url(address);
        let block_url = Self::block_url(block_hash);

        let message = format!(
            "⛏️ <b>Block Reward Confirmed</b>\n\n\
            <b>Address:</b> <a href=\"{}\">{}</a>\n\
            💰 <b>Reward:</b> <b>+{:.8} KAS</b>\n\
            🔴 <b>Previous balance:</b> {:.8} KAS\n\
            🟢 <b>New balance:</b> {:.8} KAS\n\
            🔗 <b>Block Hash:</b> <a href=\"{}\">{}</a>\n\
            📊 <b>Block DAA Score:</b> {}\n\
            🕐 <b>Timestamp:</b> {}",
            address_url,
            address,
            reward,
            previous_balance,
            new_balance,
            block_url,
            block_hash,
            block_daa_score,
            timestamp_str
        );

        self.send_message_to_chat(&chat_id.to_string(), &message)
            .await
    }

    pub async fn get_updates(&self, offset: Option<i64>) -> Result<Vec<TelegramUpdate>> {
        let url = format!("https://api.telegram.org/bot{}/getUpdates", self.bot_token);

        let mut payload = json!({
            "timeout": 30,
        });

        if let Some(offset) = offset {
            payload["offset"] = json!(offset);
        }

        let response = self
            .client
            .post(&url)
            .json(&payload)
            .send()
            .await
            .context("Failed to get updates from Telegram API")?;

        let status = response.status();
        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Telegram API returned error: {} - {}",
                status,
                error_text
            ));
        }

        let update_response: TelegramUpdateResponse = response
            .json()
            .await
            .context("Failed to parse Telegram update response")?;

        Ok(update_response.result)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TelegramUpdateResponse {
    pub ok: bool,
    pub result: Vec<TelegramUpdate>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TelegramUpdate {
    pub update_id: i64,
    pub message: Option<TelegramMessage>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TelegramMessage {
    pub message_id: i64,
    pub from: Option<TelegramUser>,
    pub chat: TelegramChat,
    pub text: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TelegramUser {
    pub id: i64,
    pub is_bot: bool,
    pub first_name: String,
    pub username: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TelegramChat {
    pub id: i64,
    #[serde(rename = "type")]
    pub chat_type: String,
}
