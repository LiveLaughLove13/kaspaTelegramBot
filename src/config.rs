use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub kaspa: KaspaConfig,
    pub telegram: TelegramConfig,
    pub notifications: NotificationConfig,
    pub confirmation: ConfirmationConfig,
    // Wallet addresses to track (optional, can also be set via environment variable)
    #[serde(default)]
    pub wallet_addresses: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KaspaConfig {
    // Kaspa node gRPC address (e.g., "127.0.0.1:16110" or "grpc://127.0.0.1:16110")
    pub node_address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelegramConfig {
    // Telegram bot token from @BotFather
    pub bot_token: String,
    // Telegram chat ID (optional - for backward compatibility)
    // If provided, addresses in wallet_addresses will be associated with this user
    // If not provided, users must add addresses via Telegram commands
    #[serde(default)]
    pub chat_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationConfig {
    // Enable incoming transaction notifications
    #[serde(default = "default_true")]
    pub incoming_tx: bool,
    // Enable outgoing transaction notifications
    #[serde(default = "default_true")]
    pub outgoing_tx: bool,
    // Enable solo mining block reward notifications
    #[serde(default = "default_true")]
    pub block_rewards: bool,
    // Only notify for blue-confirmed blocks (true) or all blocks (false)
    #[serde(default = "default_true")]
    pub blue_blocks_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfirmationConfig {
    // Minimum DAA score difference required for confirmation
    // A transaction/block is considered confirmed when:
    // virtual_daa_score - transaction_daa_score >= confirmation_depth
    #[serde(default = "default_confirmation_depth")]
    pub daa_score_depth: u64,
}

fn default_true() -> bool {
    true
}

fn default_confirmation_depth() -> u64 {
    3 // Optimized for 10 BPS network (3 DAA = 0.3 seconds)
}

impl Config {
    pub fn load() -> anyhow::Result<Self> {
        let config_path = "bot-config.toml";

        if !std::path::Path::new(config_path).exists() {
            // Create a default config file
            let default_toml = r#"# Kaspa Telegram Bot Configuration

# Kaspa node connection
[kaspa]
# Kaspa node gRPC address
# Examples: "127.0.0.1:16110" or "grpc://127.0.0.1:16110"
node_address = "127.0.0.1:16110"

# Telegram configuration
[telegram]
# Bot token from @BotFather on Telegram
bot_token = ""
# Chat ID (OPTIONAL - for backward compatibility only)
# In multi-user mode, this is only used if you want to pre-configure addresses
# If provided, addresses in wallet_addresses will be associated with this user
# If not provided, users can add addresses via Telegram commands (/add)
# Get your chat ID from @userinfobot on Telegram
chat_id = ""

# Notification settings
[notifications]
# Enable incoming transaction notifications
incoming_tx = true
# Enable outgoing transaction notifications
outgoing_tx = true
# Enable solo mining block reward notifications
block_rewards = true
# Only notify for blue-confirmed blocks (true) or all blocks (false)
# Recommended: true to avoid spam from rejected blocks
blue_blocks_only = true

# Confirmation settings
[confirmation]
# Minimum DAA score difference required for confirmation
# A transaction/block is considered confirmed when:
# virtual_daa_score - transaction_daa_score >= daa_score_depth
# Recommended: 10-20 for faster notifications, higher for more security
daa_score_depth = 10

# Wallet addresses to track (OPTIONAL - for backward compatibility)
# These addresses will be associated with the chat_id above (if provided)
# In multi-user mode, users typically add addresses via Telegram commands (/add)
# Can also be set via KASPA_WALLET_ADDRESSES environment variable
wallet_addresses = []
# Example (only works if chat_id is also configured):
# wallet_addresses = [
#     "kaspa:qpxxxxxx...",
#     "kaspa:qpyyyyyy...",
# ]
"#;
            fs::write(config_path, default_toml)?;
            anyhow::bail!(
                "Created default config at {}. Please edit with your settings and restart.",
                config_path
            );
        }

        let contents = fs::read_to_string(config_path)
            .with_context(|| format!("Failed to read config file: {}", config_path))?;
        let mut config: Config = toml::from_str(&contents)
            .with_context(|| format!("Failed to parse config file: {}", config_path))?;

        // Override bot_token from environment variable if set (security best practice)
        if let Ok(env_token) = std::env::var("TELEGRAM_BOT_TOKEN") {
            if !env_token.is_empty() {
                config.telegram.bot_token = env_token;
            }
        }

        Ok(config)
    }
}
