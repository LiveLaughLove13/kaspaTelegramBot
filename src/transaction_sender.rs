use crate::config::{WalletConfig, WalletMode};
use anyhow::{Context, Result};
use kaspa_addresses::{Address, Prefix, Version};
use kaspa_consensus_core::{
    config::params::Params,
    constants::{MAX_TX_IN_SEQUENCE_NUM, TX_VERSION, UNACCEPTED_DAA_SCORE},
    mass::MassCalculator,
    sign,
    subnets::SUBNETWORK_ID_NATIVE,
    tx::{
        PopulatedTransaction, Transaction, TransactionInput, TransactionOutpoint,
        TransactionOutput, UtxoEntry,
    },
};
use kaspa_grpc_client::GrpcClient;
use kaspa_rpc_core::{
    api::rpc::RpcApi, notify::mode::NotificationMode, GetUtxosByAddressesRequest,
};
use secp256k1::Keypair;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::Mutex;

const SOMPI_PER_KAS: u64 = 100_000_000;

#[derive(Debug)]
pub struct TransactionSender {
    enabled: bool,
    mode: WalletMode,
    node_address: String,
    signer_url: Option<String>,
    signer_api_key: Option<String>,
    allow_user_credentials: bool,
    chat_private_keys: Mutex<HashMap<i64, String>>,
    http_client: reqwest::Client,
}

#[derive(Debug, Clone, Serialize)]
pub struct SendKaspaRequest {
    pub chat_id: i64,
    pub from_address: String,
    pub to_address: String,
    pub amount_sompi: u64,
}

#[derive(Debug, Clone)]
pub struct SendKaspaResult {
    pub txid: String,
}

#[derive(Debug, Deserialize)]
struct SignerSendResponse {
    txid: Option<String>,
    transaction_id: Option<String>,
    error: Option<String>,
}

impl TransactionSender {
    pub fn new(config: &WalletConfig) -> Self {
        let enabled = config.send_enabled;
        let mode = config.mode.clone();
        let signer_url = if config.signer.url.trim().is_empty() {
            None
        } else {
            Some(config.signer.url.trim().to_string())
        };
        let signer_api_key = if config.signer.api_key.trim().is_empty() {
            None
        } else {
            Some(config.signer.api_key.trim().to_string())
        };
        let node_address = if config.node_address.trim().is_empty() {
            "grpc://127.0.0.1:16110".to_string()
        } else {
            config.node_address.trim().to_string()
        };
        let chat_private_keys =
            parse_chat_private_keys_map(&config.preconfigured_chat_private_keys);

        Self {
            enabled,
            mode,
            node_address,
            signer_url,
            signer_api_key,
            allow_user_credentials: config.allow_user_credentials,
            chat_private_keys: Mutex::new(chat_private_keys),
            http_client: reqwest::Client::builder()
                .timeout(Duration::from_secs(20))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
        }
    }

    pub async fn is_enabled(&self) -> bool {
        if !self.enabled {
            return false;
        }
        match self.mode {
            WalletMode::Watchonly => false,
            WalletMode::External => self.signer_url.is_some(),
            WalletMode::Local => {
                self.allow_user_credentials || !self.chat_private_keys.lock().await.is_empty()
            }
        }
    }

    pub fn mode(&self) -> &WalletMode {
        &self.mode
    }

    pub fn allow_user_credentials(&self) -> bool {
        self.allow_user_credentials
    }

    pub async fn set_user_private_key(&self, chat_id: i64, private_key_hex: &str) -> Result<()> {
        if !self.allow_user_credentials {
            anyhow::bail!("User wallet credential commands are disabled by operator");
        }
        let normalized = normalize_private_key_hex(private_key_hex)?;
        self.chat_private_keys
            .lock()
            .await
            .insert(chat_id, normalized);
        Ok(())
    }

    pub async fn clear_user_private_key(&self, chat_id: i64) -> bool {
        self.chat_private_keys
            .lock()
            .await
            .remove(&chat_id)
            .is_some()
    }

    pub async fn has_user_private_key(&self, chat_id: i64) -> bool {
        self.chat_private_keys.lock().await.contains_key(&chat_id)
    }

    pub async fn get_user_wallet_address(&self, chat_id: i64) -> Result<String> {
        if self.mode != WalletMode::Local {
            anyhow::bail!("User wallet address is only available in local mode");
        }
        let keypair = self.keypair_for_chat(chat_id).await?;
        let prefix = self.fetch_node_prefix().await?;
        Ok(address_from_keypair(prefix, &keypair).to_string())
    }

    pub async fn send_kaspa(&self, req: SendKaspaRequest) -> Result<SendKaspaResult> {
        if !self.enabled {
            anyhow::bail!("Send feature is disabled by server configuration");
        }

        match self.mode {
            WalletMode::External => self.send_via_external_signer(req).await,
            WalletMode::Local => self.send_via_local_wallet(req).await,
            WalletMode::Watchonly => anyhow::bail!("Wallet mode is watch-only"),
        }
    }

    async fn send_via_external_signer(&self, req: SendKaspaRequest) -> Result<SendKaspaResult> {
        let signer_url = self
            .signer_url
            .as_ref()
            .context("Signer URL is not configured")?;

        let mut builder = self.http_client.post(signer_url).json(&req);
        if let Some(api_key) = &self.signer_api_key {
            builder = builder.header("Authorization", format!("Bearer {}", api_key));
        }

        let response = builder
            .send()
            .await
            .context("Failed to reach signer service")?;
        let status = response.status();
        let body = response
            .text()
            .await
            .context("Failed to read signer response body")?;
        if !status.is_success() {
            anyhow::bail!("Signer service rejected request with status {}", status);
        }

        let parsed: SignerSendResponse =
            serde_json::from_str(&body).context("Signer response is not valid JSON")?;
        if let Some(error) = parsed.error {
            anyhow::bail!("Signer service returned error: {}", error);
        }

        let txid = parsed
            .txid
            .or(parsed.transaction_id)
            .filter(|v| !v.is_empty())
            .context("Signer response did not include txid")?;

        Ok(SendKaspaResult { txid })
    }

    async fn send_via_local_wallet(&self, req: SendKaspaRequest) -> Result<SendKaspaResult> {
        let keypair = self.keypair_for_chat(req.chat_id).await?;
        let client = self.connect_node().await?;

        let dag = client
            .get_block_dag_info()
            .await
            .context("Failed to read node DAG info")?;
        let prefix: Prefix = dag.network.into();

        let from_addr =
            Address::try_from(req.from_address.as_str()).context("Invalid from address")?;
        let to_addr = Address::try_from(req.to_address.as_str()).context("Invalid to address")?;
        if from_addr.prefix != prefix || to_addr.prefix != prefix {
            anyhow::bail!("Address network prefix does not match connected node network");
        }

        let expected_from = address_from_keypair(prefix, &keypair);
        if expected_from != from_addr {
            anyhow::bail!("Configured signing key does not match source address");
        }

        let (inputs, total_input_sompi) =
            pick_inputs_for_amount(&client, &from_addr, req.amount_sompi)
                .await
                .context("Failed selecting spendable UTXOs")?;

        let params = Params::from(dag.network);
        let mass_calc = MassCalculator::new(
            params.mass_per_tx_byte,
            params.mass_per_script_pub_key_byte,
            params.mass_per_sig_op,
            params.storage_mass_parameter,
        );
        let fee_est = client
            .get_fee_estimate()
            .await
            .context("Failed getting fee estimate")?;
        let feerate = fee_est.priority_bucket.feerate;
        if !feerate.is_finite() || feerate <= 0.0 {
            anyhow::bail!("Invalid feerate from node");
        }

        let from_spk = kaspa_txscript::pay_to_address_script(&from_addr);
        let to_spk = kaspa_txscript::pay_to_address_script(&to_addr);
        let (fee, change, mass_value) = estimate_fee_and_change(
            &mass_calc,
            feerate,
            total_input_sompi,
            inputs.len(),
            &from_spk,
            &to_spk,
            req.amount_sompi,
        )?;
        if total_input_sompi < req.amount_sompi.saturating_add(fee) {
            anyhow::bail!("Insufficient funds for amount + fee");
        }

        let tx_inputs: Vec<TransactionInput> = inputs
            .iter()
            .map(|(o, _, _)| TransactionInput::new(*o, vec![], MAX_TX_IN_SEQUENCE_NUM, 1))
            .collect();
        let mut outputs = vec![TransactionOutput::new(req.amount_sompi, to_spk)];
        if change > 0 {
            outputs.push(TransactionOutput::new(change, from_spk.clone()));
        }

        let mut tx = Transaction::new(
            TX_VERSION,
            tx_inputs,
            outputs,
            0,
            SUBNETWORK_ID_NATIVE,
            0,
            vec![],
        );

        for input in &mut tx.inputs {
            input.signature_script = vec![0u8; 66];
            input.sig_op_count = 1;
        }
        tx.finalize();
        if mass_value > 0 {
            tx.set_mass(mass_value);
        }

        let entries: Vec<UtxoEntry> = inputs
            .iter()
            .map(|(_, amount, script_pub_key)| {
                UtxoEntry::new(*amount, script_pub_key.clone(), UNACCEPTED_DAA_SCORE, false)
            })
            .collect();
        let mut signable = kaspa_consensus_core::tx::SignableTransaction::new(tx);
        for (idx, entry) in entries.into_iter().enumerate() {
            signable.entries[idx] = Some(entry);
        }
        let mut signed = sign::sign(signable, keypair);
        signed.tx.finalize();

        let txid = client
            .submit_transaction((&signed.tx).into(), false)
            .await
            .context("submit_transaction failed")?;

        Ok(SendKaspaResult {
            txid: txid.to_string(),
        })
    }
}

impl TransactionSender {
    async fn connect_node(&self) -> Result<GrpcClient> {
        let grpc_address = if self.node_address.starts_with("grpc://") {
            self.node_address.clone()
        } else {
            format!("grpc://{}", self.node_address)
        };
        GrpcClient::connect_with_args(
            NotificationMode::Direct,
            grpc_address,
            None,
            false,
            None,
            false,
            Some(180_000),
            Default::default(),
        )
        .await
        .context("Failed connecting to Kaspa RPC for send")
    }

    async fn fetch_node_prefix(&self) -> Result<Prefix> {
        let client = self.connect_node().await?;
        let dag = client
            .get_block_dag_info()
            .await
            .context("Failed to read node DAG info")?;
        Ok(dag.network.into())
    }

    async fn keypair_for_chat(&self, chat_id: i64) -> Result<Keypair> {
        let private_key_hex = {
            let keys = self.chat_private_keys.lock().await;
            keys.get(&chat_id)
                .cloned()
                .context("No private key configured for this chat_id")?
        };
        let key_bytes =
            hex::decode(&private_key_hex).context("Invalid private key hex encoding")?;
        if key_bytes.len() != 32 {
            anyhow::bail!("Private key must be 32 bytes");
        }
        Keypair::from_seckey_slice(secp256k1::SECP256K1, &key_bytes)
            .context("Invalid secp256k1 private key")
    }
}

fn address_from_keypair(prefix: Prefix, keypair: &Keypair) -> Address {
    let xonly_pk = keypair.public_key().x_only_public_key().0;
    Address::new(prefix, Version::PubKey, &xonly_pk.serialize())
}

type SelectedInput = (
    TransactionOutpoint,
    u64,
    kaspa_consensus_core::tx::ScriptPublicKey,
);

async fn pick_inputs_for_amount(
    client: &GrpcClient,
    from_addr: &Address,
    amount_sompi: u64,
) -> Result<(Vec<SelectedInput>, u64)> {
    let mut utxos = client
        .get_utxos_by_addresses_call(
            None,
            GetUtxosByAddressesRequest::new(vec![from_addr.clone()]),
        )
        .await
        .context("get_utxos_by_addresses failed (node likely missing --utxoindex)")?
        .entries;

    if utxos.is_empty() {
        anyhow::bail!("No spendable UTXOs found for source address");
    }

    utxos.sort_by_key(|u| std::cmp::Reverse(u.utxo_entry.amount));
    let mut selected = Vec::new();
    let mut total = 0u64;
    for u in utxos {
        selected.push((
            TransactionOutpoint::new(u.outpoint.transaction_id, u.outpoint.index),
            u.utxo_entry.amount,
            u.utxo_entry.script_public_key,
        ));
        total = total.saturating_add(selected.last().map(|v| v.1).unwrap_or(0));
        if total >= amount_sompi {
            break;
        }
    }
    if total < amount_sompi {
        anyhow::bail!("Insufficient funds");
    }
    Ok((selected, total))
}

fn estimate_fee_and_change(
    mass_calc: &MassCalculator,
    feerate_sompi_per_gram: f64,
    input_amount: u64,
    input_count: usize,
    input_spk: &kaspa_consensus_core::tx::ScriptPublicKey,
    to_spk: &kaspa_consensus_core::tx::ScriptPublicKey,
    send_value: u64,
) -> Result<(u64, u64, u64)> {
    if send_value == 0 {
        anyhow::bail!("Amount must be > 0");
    }
    if input_amount == 0 {
        anyhow::bail!("No input funds");
    }
    if !feerate_sompi_per_gram.is_finite() || feerate_sompi_per_gram <= 0.0 {
        anyhow::bail!("Invalid feerate");
    }

    let mut change_guess = input_amount.saturating_sub(send_value).max(1);
    let mut last_fee = 0u64;
    for _ in 0..20u32 {
        let mut outputs = vec![TransactionOutput::new(send_value, to_spk.clone())];
        outputs.push(TransactionOutput::new(change_guess, input_spk.clone()));

        let tx_inputs: Vec<TransactionInput> = (0..input_count)
            .map(|_| {
                TransactionInput::new(
                    TransactionOutpoint::new(Default::default(), 0),
                    vec![0u8; 66],
                    MAX_TX_IN_SEQUENCE_NUM,
                    1,
                )
            })
            .collect();
        let mut tx = Transaction::new(
            TX_VERSION,
            tx_inputs,
            outputs,
            0,
            SUBNETWORK_ID_NATIVE,
            0,
            vec![],
        );
        tx.finalize();
        let entries: Vec<UtxoEntry> = (0..input_count)
            .map(|_| {
                UtxoEntry::new(
                    input_amount / input_count as u64,
                    input_spk.clone(),
                    UNACCEPTED_DAA_SCORE,
                    false,
                )
            })
            .collect();
        let populated = PopulatedTransaction::new(&tx, entries);

        let non_ctx = mass_calc.calc_non_contextual_masses(&tx);
        let ctx = mass_calc
            .calc_contextual_masses(&populated)
            .ok_or_else(|| anyhow::anyhow!("Mass incomputable"))?;
        let total_mass = non_ctx
            .compute_mass
            .max(non_ctx.transient_mass)
            .max(ctx.storage_mass);
        let fee = (total_mass as f64 * feerate_sompi_per_gram).ceil() as u64;
        let required = send_value.saturating_add(fee);
        if input_amount < required {
            anyhow::bail!("Insufficient funds for amount and fee");
        }
        let new_change = input_amount.saturating_sub(required);
        if new_change == change_guess && fee == last_fee {
            return Ok((fee, new_change, ctx.storage_mass));
        }
        change_guess = new_change.max(1);
        last_fee = fee;
    }

    let required = send_value.saturating_add(last_fee);
    if input_amount < required {
        anyhow::bail!("Insufficient funds for amount and fee");
    }
    Ok((last_fee, input_amount.saturating_sub(required), 0))
}

fn parse_chat_private_keys_map(raw: &HashMap<String, String>) -> HashMap<i64, String> {
    let mut out = HashMap::new();
    for (k, v) in raw {
        let Ok(chat_id) = k.parse::<i64>() else {
            continue;
        };
        if let Ok(normalized) = normalize_private_key_hex(v) {
            out.insert(chat_id, normalized);
        }
    }
    out
}

fn normalize_private_key_hex(value: &str) -> Result<String> {
    let trimmed = value.trim();
    let normalized = trimmed.strip_prefix("0x").unwrap_or(trimmed);
    if normalized.len() != 64 {
        anyhow::bail!("Private key must be 64 hex characters");
    }
    let key_bytes = hex::decode(normalized).context("Private key must be valid hex")?;
    if key_bytes.len() != 32 {
        anyhow::bail!("Private key must decode to 32 bytes");
    }
    let _ = Keypair::from_seckey_slice(secp256k1::SECP256K1, &key_bytes)
        .context("Invalid secp256k1 private key")?;
    Ok(normalized.to_ascii_lowercase())
}

pub fn parse_kas_to_sompi(input: &str) -> Result<u64> {
    let s = input.trim();
    if s.is_empty() {
        anyhow::bail!("Amount cannot be empty");
    }

    if s.starts_with('-') {
        anyhow::bail!("Amount must be positive");
    }

    let mut parts = s.split('.');
    let whole_str = parts.next().unwrap_or_default();
    let frac_str = parts.next().unwrap_or_default();

    if parts.next().is_some() {
        anyhow::bail!("Amount format is invalid");
    }

    if whole_str.is_empty() && frac_str.is_empty() {
        anyhow::bail!("Amount format is invalid");
    }

    if !whole_str.chars().all(|c| c.is_ascii_digit()) {
        anyhow::bail!("Amount must contain only digits");
    }
    if !frac_str.chars().all(|c| c.is_ascii_digit()) {
        anyhow::bail!("Amount must contain only digits");
    }
    if frac_str.len() > 8 {
        anyhow::bail!("Amount supports up to 8 decimal places");
    }

    let whole = if whole_str.is_empty() {
        0
    } else {
        whole_str
            .parse::<u64>()
            .context("Failed to parse whole amount")?
    };
    let frac = if frac_str.is_empty() {
        0
    } else {
        let padded = format!("{:0<8}", frac_str);
        padded
            .parse::<u64>()
            .context("Failed to parse fractional amount")?
    };

    let whole_sompi = whole
        .checked_mul(SOMPI_PER_KAS)
        .context("Amount is too large")?;
    let total = whole_sompi
        .checked_add(frac)
        .context("Amount is too large")?;

    if total == 0 {
        anyhow::bail!("Amount must be greater than zero");
    }

    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::{normalize_private_key_hex, parse_chat_private_keys_map, parse_kas_to_sompi};
    use std::collections::HashMap;

    #[test]
    fn parses_decimal_amount() {
        assert_eq!(parse_kas_to_sompi("1.23").unwrap(), 123_000_000);
        assert_eq!(parse_kas_to_sompi("0.00000001").unwrap(), 1);
        assert_eq!(parse_kas_to_sompi("2").unwrap(), 200_000_000);
        assert_eq!(parse_kas_to_sompi("1.").unwrap(), 100_000_000);
        assert_eq!(parse_kas_to_sompi(".5").unwrap(), 50_000_000);
        assert_eq!(parse_kas_to_sompi(" 0.10000000 ").unwrap(), 10_000_000);
    }

    #[test]
    fn rejects_invalid_amounts() {
        assert!(parse_kas_to_sompi("0").is_err());
        assert!(parse_kas_to_sompi("-1").is_err());
        assert!(parse_kas_to_sompi("1.000000001").is_err());
        assert!(parse_kas_to_sompi("abc").is_err());
        assert!(parse_kas_to_sompi("").is_err());
        assert!(parse_kas_to_sompi("1.2.3").is_err());
        assert!(parse_kas_to_sompi("+1").is_err());
    }

    #[test]
    fn parses_chat_key_map() {
        let mut raw = HashMap::new();
        raw.insert(
            "123".to_string(),
            "0000000000000000000000000000000000000000000000000000000000000001".to_string(),
        );
        raw.insert(
            "-99".to_string(),
            "0x0000000000000000000000000000000000000000000000000000000000000002".to_string(),
        );
        let keys = parse_chat_private_keys_map(&raw);
        assert_eq!(
            keys.get(&123).map(String::as_str),
            Some("0000000000000000000000000000000000000000000000000000000000000001")
        );
        assert_eq!(
            keys.get(&-99).map(String::as_str),
            Some("0000000000000000000000000000000000000000000000000000000000000002")
        );
    }

    #[test]
    fn validates_private_key_hex() {
        assert!(normalize_private_key_hex("01").is_err());
        assert!(normalize_private_key_hex(
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
        )
        .is_err());
        assert_eq!(
            normalize_private_key_hex(
                "0x0000000000000000000000000000000000000000000000000000000000000003"
            )
            .unwrap(),
            "0000000000000000000000000000000000000000000000000000000000000003"
        );
    }

    #[test]
    fn parse_chat_key_map_skips_invalid_entries() {
        let mut raw = HashMap::new();
        raw.insert("not-a-chat-id".to_string(), "00".to_string());
        raw.insert(
            "42".to_string(),
            "invalid-private-key-value-that-is-not-hex".to_string(),
        );
        let keys = parse_chat_private_keys_map(&raw);
        assert!(keys.is_empty());
    }
}
