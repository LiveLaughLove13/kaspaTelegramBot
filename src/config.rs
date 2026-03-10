use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub kaspa: KaspaConfig,
    pub telegram: TelegramConfig,
    pub notifications: NotificationConfig,
    pub confirmation: ConfirmationConfig,
    #[serde(default)]
    pub wallet: WalletConfig,
    #[serde(default)]
    pub ai: AiConfig,
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
    // Enforce that block reward notifications reconcile with observed balance increase
    #[serde(default = "default_true")]
    pub strict_balance_reconciliation: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum WalletMode {
    #[default]
    Watchonly,
    Local,
    External,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WalletSignerConfig {
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub api_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletConfig {
    #[serde(default)]
    pub send_enabled: bool,
    #[serde(default)]
    pub mode: WalletMode,
    #[serde(default)]
    pub node_address: String,
    #[serde(default = "default_true")]
    pub allow_user_credentials: bool,
    #[serde(default)]
    pub preconfigured_chat_private_keys: HashMap<String, String>,
    #[serde(default)]
    pub signer: WalletSignerConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_ai_payment_address")]
    pub payment_address: String,
    #[serde(default = "default_ai_minimum_payment_kas")]
    pub minimum_payment_kas: f64,
    #[serde(default)]
    pub local_model: LocalModelConfig,
    #[serde(default)]
    pub knowledge: AiKnowledgeConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalModelConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_local_model_endpoint")]
    pub endpoint: String,
    #[serde(default = "default_local_model_name")]
    pub model: String,
    #[serde(default = "default_local_model_timeout_seconds")]
    pub timeout_seconds: u64,
}

/// Single repository entry for multi-repo knowledge base
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnowledgeRepoEntry {
    pub url: String,
    /// Subdirectory name under base_path (e.g. "rusty-kaspa")
    pub path: String,
    #[serde(default = "default_branch")]
    pub branch: String,
}

fn default_branch() -> String {
    "master".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiKnowledgeConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Base directory for all repos (e.g. "tools/knowledge")
    #[serde(default = "default_ai_knowledge_base_path")]
    pub base_path: String,
    /// Multi-repo list. If non-empty, used instead of repo_url/local_path
    #[serde(default)]
    pub repos: Vec<KnowledgeRepoEntry>,
    #[serde(default = "default_ai_knowledge_repo_url")]
    pub repo_url: String,
    #[serde(default = "default_ai_knowledge_local_path")]
    pub local_path: String,
    #[serde(default = "default_ai_knowledge_refresh_minutes")]
    pub refresh_minutes: u64,
    #[serde(default = "default_ai_knowledge_max_context_chars")]
    pub max_context_chars: usize,
    #[serde(default = "default_ai_knowledge_git_timeout_seconds")]
    pub git_timeout_seconds: u64,
}

impl Default for AiConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            payment_address: default_ai_payment_address(),
            minimum_payment_kas: default_ai_minimum_payment_kas(),
            local_model: LocalModelConfig::default(),
            knowledge: AiKnowledgeConfig::default(),
        }
    }
}

impl Default for LocalModelConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            endpoint: default_local_model_endpoint(),
            model: default_local_model_name(),
            timeout_seconds: default_local_model_timeout_seconds(),
        }
    }
}

fn default_ai_knowledge_base_path() -> String {
    "tools/knowledge".to_string()
}

impl Default for AiKnowledgeConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            base_path: default_ai_knowledge_base_path(),
            repos: Vec::new(),
            repo_url: default_ai_knowledge_repo_url(),
            local_path: default_ai_knowledge_local_path(),
            refresh_minutes: default_ai_knowledge_refresh_minutes(),
            max_context_chars: default_ai_knowledge_max_context_chars(),
            git_timeout_seconds: default_ai_knowledge_git_timeout_seconds(),
        }
    }
}

/// Resolved repo: (url, full_local_path, branch, github_base_url for source links)
pub type ResolvedKnowledgeRepo = (String, std::path::PathBuf, String, String);

impl AiKnowledgeConfig {
    /// Returns list of repos to sync. Uses repos if non-empty, else legacy single repo.
    pub fn resolved_repos(&self) -> Vec<ResolvedKnowledgeRepo> {
        if self.repos.is_empty() {
            let cwd = std::env::current_dir().unwrap_or_default();
            let path = if self.local_path.starts_with('/') || (cfg!(windows) && self.local_path.len() > 1 && self.local_path.chars().nth(1) == Some(':')) {
                std::path::PathBuf::from(&self.local_path)
            } else {
                cwd.join(&self.local_path)
            };
            let github_base = repo_url_to_github_base(&self.repo_url);
            return vec![(
                self.repo_url.clone(),
                path,
                "master".to_string(),
                github_base,
            )];
        }
        let cwd = std::env::current_dir().unwrap_or_default();
        let base = if self.base_path.starts_with('/') || (cfg!(windows) && self.base_path.len() > 1 && self.base_path.chars().nth(1) == Some(':')) {
            std::path::PathBuf::from(&self.base_path)
        } else {
            cwd.join(&self.base_path)
        };
        self.repos
            .iter()
            .map(|r| {
                let full_path = base.join(&r.path);
                let github_base = repo_url_to_github_base(&r.url);
                (r.url.clone(), full_path, r.branch.clone(), github_base)
            })
            .collect()
    }
}

fn repo_url_to_github_base(url: &str) -> String {
    url.trim_end_matches(".git").trim_end_matches('/').to_string()
}

impl Default for WalletConfig {
    fn default() -> Self {
        Self {
            send_enabled: false,
            mode: WalletMode::Watchonly,
            node_address: String::new(),
            allow_user_credentials: true,
            preconfigured_chat_private_keys: HashMap::new(),
            signer: WalletSignerConfig::default(),
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_confirmation_depth() -> u64 {
    3 // Optimized for 10 BPS network (3 DAA = 0.3 seconds)
}

fn default_ai_payment_address() -> String {
    "kaspa:qp44zy8snd2rf6zw5eenv0jxkn3h6ppv0mfsrp5l3kpjdqlsylknj5exp24mz".to_string()
}

fn default_ai_minimum_payment_kas() -> f64 {
    0.1
}

fn default_local_model_endpoint() -> String {
    "http://127.0.0.1:11434/api/generate".to_string()
}

fn default_local_model_name() -> String {
    "qwen2.5-coder:7b".to_string()
}

fn default_local_model_timeout_seconds() -> u64 {
    45
}

fn default_ai_knowledge_repo_url() -> String {
    "https://github.com/kaspanet/rusty-kaspa.git".to_string()
}

fn default_ai_knowledge_local_path() -> String {
    "tools/knowledge/rusty-kaspa".to_string()
}

fn default_ai_knowledge_refresh_minutes() -> u64 {
    30
}

fn default_ai_knowledge_max_context_chars() -> usize {
    8000
}

fn default_ai_knowledge_git_timeout_seconds() -> u64 {
    20
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
# Require reward notifications to match observable balance increase
strict_balance_reconciliation = true

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

# Wallet send mode
[wallet]
# Master switch for /send
send_enabled = false
# Modes: "watchonly", "local", "external"
mode = "watchonly"
# RPC node used for sending in local mode (defaults to [kaspa].node_address if empty)
node_address = ""
# Allow users to provide their own private key credentials via Telegram command
allow_user_credentials = true
# Optional operator-provided keys per chat ID (string chat_id -> 64-char hex private key)
preconfigured_chat_private_keys = {}
# External signer settings (used when mode = "external")
[wallet.signer]
url = ""
api_key = ""

# AI Q&A settings (payment-gated)
[ai]
enabled = false
# User must pay at least this amount to ask one AI question
minimum_payment_kas = 0.1
# Payment destination address
payment_address = "kaspa:qp44zy8snd2rf6zw5eenv0jxkn3h6ppv0mfsrp5l3kpjdqlsylknj5exp24mz"

[ai.local_model]
enabled = true
endpoint = "http://127.0.0.1:11434/api/generate"
model = "qwen2.5-coder:7b"
timeout_seconds = 45

[ai.knowledge]
enabled = true
repo_url = "https://github.com/kaspanet/rusty-kaspa.git"
local_path = "tools/knowledge/rusty-kaspa"
refresh_minutes = 30
max_context_chars = 8000
git_timeout_seconds = 20
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

        if config.wallet.node_address.trim().is_empty() {
            config.wallet.node_address = config.kaspa.node_address.clone();
        }

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::{Config, WalletConfig, WalletMode};
    use std::fs;
    use std::path::PathBuf;
    use std::sync::{Mutex, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("{}_{}", prefix, nanos))
    }

    #[test]
    fn wallet_mode_default_is_watchonly() {
        assert_eq!(WalletMode::default(), WalletMode::Watchonly);
    }

    #[test]
    fn wallet_config_defaults_are_safe() {
        let wallet = WalletConfig::default();
        assert!(!wallet.send_enabled);
        assert_eq!(wallet.mode, WalletMode::Watchonly);
        assert!(wallet.node_address.is_empty());
        assert!(wallet.allow_user_credentials);
        assert!(wallet.preconfigured_chat_private_keys.is_empty());
        assert!(wallet.signer.url.is_empty());
        assert!(wallet.signer.api_key.is_empty());
    }

    #[test]
    fn load_creates_default_config_when_missing() {
        let _guard = test_lock().lock().expect("test lock poisoned");
        let prev_dir = std::env::current_dir().expect("read cwd");
        let prev_token = std::env::var("TELEGRAM_BOT_TOKEN").ok();
        std::env::remove_var("TELEGRAM_BOT_TOKEN");

        let dir = unique_temp_dir("kaspa_bot_config_missing");
        fs::create_dir_all(&dir).expect("create temp dir");
        std::env::set_current_dir(&dir).expect("set cwd");

        let result = Config::load();
        assert!(result.is_err());
        let err = format!("{}", result.expect_err("expected missing-config error"));
        assert!(err.contains("Created default config at bot-config.toml"));
        assert!(dir.join("bot-config.toml").exists());

        std::env::set_current_dir(prev_dir).expect("restore cwd");
        if let Some(token) = prev_token {
            std::env::set_var("TELEGRAM_BOT_TOKEN", token);
        }
    }

    #[test]
    fn load_applies_env_token_and_wallet_node_fallback() {
        let _guard = test_lock().lock().expect("test lock poisoned");
        let prev_dir = std::env::current_dir().expect("read cwd");
        let prev_token = std::env::var("TELEGRAM_BOT_TOKEN").ok();

        let dir = unique_temp_dir("kaspa_bot_config_fallback");
        fs::create_dir_all(&dir).expect("create temp dir");
        std::env::set_current_dir(&dir).expect("set cwd");

        let cfg = r#"
[kaspa]
node_address = "127.0.0.1:16110"

[telegram]
bot_token = "file-token"
chat_id = ""

[notifications]
incoming_tx = true
outgoing_tx = true
block_rewards = true
blue_blocks_only = true

[confirmation]
daa_score_depth = 10
strict_balance_reconciliation = true

[wallet]
send_enabled = false
mode = "watchonly"
node_address = ""
allow_user_credentials = true
preconfigured_chat_private_keys = {}

[wallet.signer]
url = ""
api_key = ""
"#;
        fs::write("bot-config.toml", cfg).expect("write config");
        std::env::set_var("TELEGRAM_BOT_TOKEN", "env-token");

        let loaded = Config::load().expect("load config");
        assert_eq!(loaded.telegram.bot_token, "env-token");
        assert_eq!(loaded.wallet.node_address, loaded.kaspa.node_address);

        std::env::set_current_dir(prev_dir).expect("restore cwd");
        if let Some(token) = prev_token {
            std::env::set_var("TELEGRAM_BOT_TOKEN", token);
        } else {
            std::env::remove_var("TELEGRAM_BOT_TOKEN");
        }
    }

    #[test]
    fn load_keeps_explicit_wallet_node_address() {
        let _guard = test_lock().lock().expect("test lock poisoned");
        let prev_dir = std::env::current_dir().expect("read cwd");
        let prev_token = std::env::var("TELEGRAM_BOT_TOKEN").ok();
        std::env::remove_var("TELEGRAM_BOT_TOKEN");

        let dir = unique_temp_dir("kaspa_bot_config_explicit_wallet_node");
        fs::create_dir_all(&dir).expect("create temp dir");
        std::env::set_current_dir(&dir).expect("set cwd");

        let cfg = r#"
[kaspa]
node_address = "127.0.0.1:16110"

[telegram]
bot_token = "file-token"
chat_id = ""

[notifications]
incoming_tx = true
outgoing_tx = true
block_rewards = true
blue_blocks_only = true

[confirmation]
daa_score_depth = 10
strict_balance_reconciliation = true

[wallet]
send_enabled = true
mode = "local"
node_address = "127.0.0.1:26210"
allow_user_credentials = true
preconfigured_chat_private_keys = {}

[wallet.signer]
url = ""
api_key = ""
"#;
        fs::write("bot-config.toml", cfg).expect("write config");

        let loaded = Config::load().expect("load config");
        assert_eq!(loaded.wallet.node_address, "127.0.0.1:26210");
        assert_eq!(loaded.kaspa.node_address, "127.0.0.1:16110");

        std::env::set_current_dir(prev_dir).expect("restore cwd");
        if let Some(token) = prev_token {
            std::env::set_var("TELEGRAM_BOT_TOKEN", token);
        }
    }
}
