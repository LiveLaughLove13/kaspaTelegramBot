# Kaspa Telegram Bot

A standalone Telegram bot for receiving notifications about confirmed Kaspa transactions and block rewards.

## Features

- ✅ **Confirmed Events Only**: Only sends notifications after transactions/blocks are fully confirmed in the DAG
- ✅ **Configurable Confirmation Depth**: Set the minimum DAA score difference required for confirmation
- ✅ **Incoming Transactions**: Get notified when you receive KAS
- ✅ **Outgoing Transactions**: Get notified when you send KAS (including fees)
- ✅ **Solo Mining Block Rewards**: Get notified when you mine a block
- ✅ **Per-Notification Toggle**: Enable/disable each notification type separately
- ✅ **Balance Tracking**: Shows previous balance → new balance for transactions
- ✅ **Interactive Commands**: Add/remove tracked addresses via Telegram messages
- ✅ **Address Management**: List all tracked addresses and their balances

## Requirements

- Rust 1.70+ (edition 2021)
- Access to a Kaspa node (local or remote) with gRPC enabled
- Telegram bot token from [@BotFather](https://t.me/BotFather)
- Telegram chat ID (your user ID or group ID)

## Installation

1. Clone or download this repository
2. Run the bot once - it will create `bot-config.toml` with default settings
3. Edit `bot-config.toml` with your settings:
   - Kaspa node address
   - Telegram bot token
   - Telegram chat ID (optional for multi-user mode)
   - Notification preferences
   - Confirmation depth

## Configuration

The bot uses `bot-config.toml` in the current directory. On first run, it will create a default config file that you can edit.

### Quick Setup

See **[SETUP_GUIDE.md](SETUP_GUIDE.md)** for detailed step-by-step instructions on:
- Creating a Telegram bot and getting your bot token
- Finding your chat ID (for personal chats or groups)
- Configuring the bot

### Quick Reference

**Getting Bot Token:**
1. Message [@BotFather](https://t.me/BotFather) on Telegram
2. Send `/newbot` and follow instructions
3. Copy the token provided

**Getting Chat ID:**
- **Personal:** Message [@userinfobot](https://t.me/userinfobot) - it shows your user ID
- **Group:** Add [@userinfobot](https://t.me/userinfobot) to your group - it shows the group ID (negative number)

### Wallet Addresses

Wallet addresses can be configured in three ways:

**Option 1: Telegram Commands (Easiest)**
Once the bot is running, send it messages:
- Send a Kaspa address directly: `kaspa:qpxxxxxx...`
- Use `/add kaspa:qpxxxxxx...` to add an address
- Use `/remove kaspa:qpxxxxxx...` to remove an address
- Use `/list` to see all tracked addresses

**Option 2: Config File**
Add addresses to your `bot-config.toml`:
```toml
wallet_addresses = [
    "kaspa:xxxx...",
    "kaspa:yyyy...",
    "kaspa:zzzz...",
]
```

**Option 3: Environment Variable**
```bash
export KASPA_WALLET_ADDRESSES="kaspa:xxxx,kaspa:yyyy,kaspa:zzzz"
```

Or on Windows PowerShell:
```powershell
$env:KASPA_WALLET_ADDRESSES="kaspa:xxxx,kaspa:yyyy,kaspa:zzzz"
```

### Telegram Commands

Once the bot is running, you can interact with it via Telegram:

- **`/start`** or **`/help`** - Show available commands
- **`/add <address>`** - Add a Kaspa address to track
- **`/remove <address>`** - Remove a tracked address
- **`/list`** - List all tracked addresses with their balances

**Quick Add:** Just send a Kaspa address directly (e.g., `kaspa:qpxxxxxx...`) and the bot will automatically start tracking it!

## Building

```bash
cd D:\kaspa-telegram-bot
cargo build --release
```

The binary will be at `target/release/kaspa-telegram-bot.exe`

### Build Issues

If you encounter a `wasm-bindgen` version error:

**Quick Fix (PowerShell):**
```powershell
.\fix-wasm-bindgen.ps1
```

**Manual Fix:**
1. Update Rust: `rustup update stable`
2. Clean build: `cargo clean && cargo build --release`

See `BUILD_NOTES.md` for more detailed troubleshooting.

## Running

```bash
# The bot will look for bot-config.toml in the current directory
cargo run --release
```

The bot will automatically create `bot-config.toml` with default settings if it doesn't exist. Edit the file with your settings before running.

## How It Works

1. **Connects to Kaspa Node**: The bot connects to your Kaspa node via gRPC
2. **Subscribes to Notifications**: Subscribes to `BlockAdded` and `UtxosChanged` notifications
3. **Tracks Wallet Addresses**: Monitors specified wallet addresses for activity
4. **Checks Confirmation**: When a transaction/block is detected, it checks if it's confirmed by comparing DAA scores
5. **Sends Notifications**: Only sends Telegram notifications for confirmed events

### Confirmation Logic

A transaction or block is considered **confirmed** when:
```
virtual_daa_score - transaction_daa_score >= daa_score_depth
```

Where:
- `virtual_daa_score`: Current virtual DAA score from the node
- `transaction_daa_score`: DAA score when the transaction/block was included
- `daa_score_depth`: Configurable confirmation depth (default: 10)

This ensures notifications are only sent for transactions/blocks that are deeply embedded in the DAG, reducing the risk of false positives from reorgs.

## Notification Formats

### Incoming Transaction
```
📥 Incoming Transaction Confirmed

Address: kaspa:xxxx
Amount: +X.XX KAS
Balance: Y.YY KAS → Z.ZZ KAS
TXID: abc123...
DAA Score: 12345
Timestamp: 2026-02-07 12:34:56 UTC
```

### Outgoing Transaction
```
📤 Outgoing Transaction Confirmed

Address: kaspa:xxxx
Amount: -X.XX KAS
Fee: 0.00001 KAS
Balance: Y.YY KAS → Z.ZZ KAS
TXID: abc123...
DAA Score: 12345
Timestamp: 2026-02-07 12:34:56 UTC
```

### Block Reward
```
⛏️ Block Reward Confirmed

Address: kaspa:xxxx
Reward: +X.XX KAS
Block Hash: abc123...
Block DAA Score: 12345
Timestamp: 2026-02-07 12:34:56 UTC
```

## Troubleshooting

### Bot Not Sending Messages

1. **Check Bot Token**: Ensure your bot token is correct
2. **Check Chat ID**: Ensure your chat ID is correct (positive for users, negative for groups)
3. **Start Bot First**: Make sure you've started a conversation with your bot first
4. **Check Logs**: Look for error messages in the console output

### Not Receiving Notifications

1. **Check Addresses**: Ensure wallet addresses are correctly configured
2. **Check Node Connection**: Verify the bot can connect to your Kaspa node
3. **Check Notification Settings**: Ensure the notification type is enabled in config
4. **Check Confirmation Depth**: If depth is too high, notifications may be delayed

### Connection Issues

1. **Node Address**: Ensure the node address is correct (include port if needed)
2. **gRPC Enabled**: Ensure your Kaspa node has gRPC enabled
3. **Firewall**: Check if firewall is blocking the connection

## License

This project is provided as-is for use with the Kaspa network.

