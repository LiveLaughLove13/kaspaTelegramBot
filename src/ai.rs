use crate::config::AiConfig;
use crate::kaspa_client::KaspaClient;
use crate::transaction_sender::parse_kas_to_sompi;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info};

#[derive(Debug, Clone)]
pub struct AiPaymentRequest {
    pub payment_address: String,
    pub minimum_payment_kas: f64,
}

pub struct AiService {
    config: AiConfig,
    client: reqwest::Client,
    pending_questions: Arc<Mutex<HashMap<i64, String>>>,
    consumed_txids: Arc<Mutex<HashSet<String>>>,
    conversation_memory: Arc<Mutex<HashMap<i64, Vec<ConversationTurn>>>>,
    knowledge_index: Arc<Mutex<Option<KnowledgeIndex>>>,
}

impl AiService {
    pub fn new(config: &AiConfig) -> Self {
        Self {
            config: config.clone(),
            client: reqwest::Client::new(),
            pending_questions: Arc::new(Mutex::new(HashMap::new())),
            consumed_txids: Arc::new(Mutex::new(HashSet::new())),
            conversation_memory: Arc::new(Mutex::new(HashMap::new())),
            knowledge_index: Arc::new(Mutex::new(None)),
        }
    }

    pub fn enabled(&self) -> bool {
        self.config.enabled
    }

    pub async fn status_report(&self) -> String {
        let mut lines = vec![
            "🤖 <b>AI Runtime Status</b>".to_string(),
            format!("AI enabled: <code>{}</code>", self.config.enabled),
            format!(
                "Local model enabled: <code>{}</code>",
                self.config.local_model.enabled
            ),
            format!(
                "Local model endpoint: <code>{}</code>",
                self.config.local_model.endpoint
            ),
            format!(
                "Local model name: <code>{}</code>",
                self.config.local_model.model
            ),
            format!(
                "Knowledge enabled: <code>{}</code>",
                self.config.knowledge.enabled
            ),
            format!(
                "Knowledge refresh: <code>{} min</code>",
                self.config.knowledge.refresh_minutes
            ),
        ];

        let model_health = self.local_model_health().await;
        lines.push(format!("Local model health: <code>{}</code>", model_health));

        let repos = self.config.knowledge.resolved_repos();
        if repos.is_empty() {
            lines.push("Knowledge repos: <code>none</code>".to_string());
        } else {
            lines.push(format!("Knowledge repos: <code>{}</code>", repos.len()));
            if let Some((_, p, _, _)) = repos.first() {
                if let Some(base) = p.parent() {
                    lines.push(format!("Knowledge base: <code>{}</code>", base.display()));
                }
            }
            for (_, path, _, _) in repos.iter().take(5) {
                let exists = path.exists();
                let head = if exists {
                    self.repo_head_short(path).await.unwrap_or_default()
                } else {
                    String::new()
                };
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("?");
                lines.push(format!(
                    "  • {}: <code>{}</code> {}",
                    name,
                    if head.is_empty() { "—" } else { &head },
                    if exists { "✓" } else { "✗" }
                ));
            }
            if repos.len() > 5 {
                lines.push(format!("  ... and {} more", repos.len() - 5));
            }
        }

        lines.join("\n")
    }

    pub fn knowledge_refresh_minutes(&self) -> u64 {
        self.config.knowledge.refresh_minutes.max(1)
    }

    pub async fn sync_knowledge_repo(&self) -> Result<()> {
        if !self.config.knowledge.enabled {
            return Ok(());
        }

        let repos = self.config.knowledge.resolved_repos();
        for (url, repo_path, branch, _) in &repos {
            let git_dir = repo_path.join(".git");
            if git_dir.exists() {
                let lock_file = git_dir.join("index.lock");
                if lock_file.exists() {
                    let _ = tokio::fs::remove_file(&lock_file).await;
                }
                let fetch = tokio::time::timeout(
                    std::time::Duration::from_secs(self.config.knowledge.git_timeout_seconds),
                    tokio::process::Command::new("git")
                        .arg("-C")
                        .arg(repo_path)
                        .arg("fetch")
                        .arg("origin")
                        .arg(branch)
                        .output(),
                )
                .await
                .context("Timed out while running git fetch for AI knowledge repo")?
                .context("Failed to run git fetch for AI knowledge repo")?;
                if !fetch.status.success() {
                    let stderr = String::from_utf8_lossy(&fetch.stderr);
                    anyhow::bail!(
                        "AI knowledge git fetch failed for {}: {}",
                        repo_path.display(),
                        stderr.trim()
                    );
                }
                let origin_ref = format!("origin/{}", branch);
                let reset = tokio::time::timeout(
                    std::time::Duration::from_secs(self.config.knowledge.git_timeout_seconds),
                    tokio::process::Command::new("git")
                        .arg("-C")
                        .arg(repo_path)
                        .arg("reset")
                        .arg("--hard")
                        .arg(&origin_ref)
                        .output(),
                )
                .await
                .context("Timed out while running git reset for AI knowledge repo")?
                .context("Failed to run git reset for AI knowledge repo")?;
                if !reset.status.success() {
                    let stderr = String::from_utf8_lossy(&reset.stderr);
                    anyhow::bail!(
                        "AI knowledge git reset failed for {}: {}",
                        repo_path.display(),
                        stderr.trim()
                    );
                }
                info!("AI knowledge repo refreshed at {}", repo_path.display());
            } else {
                if let Some(parent) = repo_path.parent() {
                    tokio::fs::create_dir_all(parent)
                        .await
                        .with_context(|| format!("Failed to create {}", parent.display()))?;
                }
                let output = tokio::time::timeout(
                    std::time::Duration::from_secs(self.config.knowledge.git_timeout_seconds),
                    tokio::process::Command::new("git")
                        .arg("clone")
                        .arg("--depth")
                        .arg("1")
                        .arg("--branch")
                        .arg(branch)
                        .arg(url)
                        .arg(repo_path)
                        .output(),
                )
                .await
                .context("Timed out while running git clone for AI knowledge repo")?
                .context("Failed to run git clone for AI knowledge repo")?;
                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    anyhow::bail!(
                        "AI knowledge git clone failed for {}: {}",
                        url,
                        stderr.trim()
                    );
                }
                info!(
                    "AI knowledge repo cloned from {} to {}",
                    url,
                    repo_path.display()
                );
            }
        }
        *self.knowledge_index.lock().await = None;
        Ok(())
    }

    pub async fn begin_question(&self, chat_id: i64, question: &str) -> Result<AiPaymentRequest> {
        let mut pending = self.pending_questions.lock().await;
        pending.insert(chat_id, question.trim().to_string());
        Ok(AiPaymentRequest {
            payment_address: self.config.payment_address.clone(),
            minimum_payment_kas: self.config.minimum_payment_kas,
        })
    }

    pub fn payment_request(&self) -> AiPaymentRequest {
        AiPaymentRequest {
            payment_address: self.config.payment_address.clone(),
            minimum_payment_kas: self.config.minimum_payment_kas,
        }
    }

    pub async fn has_pending_question(&self, chat_id: i64) -> bool {
        self.pending_questions.lock().await.contains_key(&chat_id)
    }

    pub async fn verify_payment_and_answer(
        &self,
        chat_id: i64,
        txid: &str,
        kaspa_client: &KaspaClient,
    ) -> Result<Option<String>> {
        let question = {
            let pending = self.pending_questions.lock().await;
            pending.get(&chat_id).cloned()
        };

        let Some(question) = question else {
            return Ok(None);
        };

        let txid_normalized = txid.trim().to_lowercase();
        let already_consumed = self.consumed_txids.lock().await.contains(&txid_normalized);
        if already_consumed {
            return Err(anyhow::anyhow!(
                "This TXID was already used for a previous AI request. Each payment can only be used once."
            ));
        }

        let paid_amount = kaspa_client
            .get_output_amount_to_address(&txid_normalized, &self.config.payment_address)
            .await?
            .unwrap_or(0);

        let minimum_payment_sompi =
            parse_kas_to_sompi(&format!("{:.8}", self.config.minimum_payment_kas))
                .context("Invalid ai.minimum_payment_kas configuration")?;
        if paid_amount < minimum_payment_sompi {
            return Err(anyhow::anyhow!(
                "Payment not found yet or amount is below minimum. Required: {:.8} KAS. If transaction is very recent, wait a few seconds and retry.",
                self.config.minimum_payment_kas
            ));
        }

        let answer = self.generate_answer(chat_id, &question).await?;

        self.pending_questions.lock().await.remove(&chat_id);
        self.consumed_txids.lock().await.insert(txid_normalized);
        self.push_memory_turn(chat_id, question, answer.clone())
            .await;

        Ok(Some(answer))
    }

    async fn generate_answer(&self, chat_id: i64, question: &str) -> Result<String> {
        let lq = question.trim().to_lowercase();
        if Self::is_bridge_attribution_prompt(&lq) {
            return Ok(self.answer_bridge_attribution(question).await);
        }

        let (knowledge_context, knowledge_sources) = self
            .retrieve_local_knowledge_context(question)
            .await
            .unwrap_or_else(|e| {
                debug!("Local knowledge retrieval skipped: {}", e);
                (String::new(), Vec::new())
            });

        let mut official_context = self.collect_official_context(question).await;
        if !knowledge_context.is_empty() {
            if !official_context.is_empty() {
                official_context.push_str("\n\n");
            }
            official_context.push_str("Local rusty-kaspa code context:\n");
            official_context.push_str(&knowledge_context);
        }
        let sources = Self::extract_sources_from_context(&official_context);
        let mut combined_sources = sources;
        for src in knowledge_sources {
            if !combined_sources.contains(&src) {
                combined_sources.push(src);
            }
        }

        if self.config.local_model.enabled {
            if let Ok(answer) = self
                .answer_with_local_model(chat_id, question, &official_context, &combined_sources)
                .await
            {
                let trimmed = answer.trim();
                if !trimmed.is_empty() {
                    let cited = Self::extract_cited_source_indexes(trimmed, combined_sources.len());
                    return Ok(Self::ensure_sources_section(
                        trimmed,
                        &combined_sources,
                        &cited,
                    ));
                }
            }
        }

        if lq.contains("contributor")
            || lq.contains("contributors")
            || lq.contains("who works on kaspa")
            || lq.contains("who built kaspa")
        {
            return Ok(self.answer_kaspa_contributors().await);
        }

        if official_context.is_empty() {
            if let Some(live_answer) = self.answer_from_kaspanet_sources(question).await {
                return Ok(live_answer);
            }
        } else {
            // Reuse gathered context to produce a deterministic non-generic fallback.
            official_context.truncate(1200);
            return Ok(format!(
                "I could not run the full local model response, but here is official Kaspa context related to your question:\n\n{}",
                official_context
            ));
        }

        Ok(self.generate_builtin_answer(question))
    }

    fn is_bridge_attribution_prompt(lq: &str) -> bool {
        let asks_people = lq.contains("who wrote")
            || lq.contains("who made")
            || lq.contains("main contributor")
            || lq.contains("contributors")
            || lq.contains("who contributed")
            || lq.contains("who worked");
        (asks_people && lq.contains("bridge")) || Self::extract_commit_sha(lq).is_some()
    }

    async fn answer_bridge_attribution(&self, question: &str) -> String {
        if let Some(sha) = Self::extract_commit_sha(question) {
            return match self.fetch_commit_context(&sha).await {
                Ok(ctx) => format!(
                    "{}\n\nInterpretation:\n- This commit was authored by the identity shown above.\n- A single commit author does not automatically mean the person is the top overall bridge contributor.\n- To identify main contributors, use aggregate bridge-related commit history.",
                    ctx
                ),
                Err(_) => "I could not validate that commit hash from the official repository right now. Please verify the SHA and try again."
                    .to_string(),
            };
        }

        match self.fetch_bridge_contributors_context().await {
            Ok(ctx) => format!(
                "{}\n\nNote:\n- This ranking is based on recent bridge-related commits in the sampled history.\n- A single commit can still belong to another contributor and be valid.",
                ctx
            ),
            Err(_) => "I could not fetch bridge contributor attribution data right now. Please try again in a few seconds."
                .to_string(),
        }
    }

    async fn push_memory_turn(&self, chat_id: i64, question: String, answer: String) {
        const MAX_TURNS: usize = 6;
        let mut memory = self.conversation_memory.lock().await;
        let chat_memory = memory.entry(chat_id).or_default();
        chat_memory.push(ConversationTurn { question, answer });
        if chat_memory.len() > MAX_TURNS {
            let excess = chat_memory.len() - MAX_TURNS;
            chat_memory.drain(0..excess);
        }
    }

    async fn collect_official_context(&self, question: &str) -> String {
        let mut ctx_parts = Vec::new();
        let lq = question.to_lowercase();
        if lq.contains("contributor")
            || lq.contains("contributors")
            || lq.contains("who works on kaspa")
            || lq.contains("who built kaspa")
        {
            if let Ok(text) = self.fetch_contributors_context().await {
                ctx_parts.push(text);
            }
        }

        if let Ok(text) = self.fetch_repo_context(question).await {
            ctx_parts.push(text);
        }

        if lq.contains("bridge") {
            if let Ok(text) = self.fetch_bridge_contributors_context().await {
                ctx_parts.push(text);
            }
        }

        if let Some(commit_sha) = Self::extract_commit_sha(question) {
            if let Ok(text) = self.fetch_commit_context(&commit_sha).await {
                ctx_parts.push(text);
            }
        }

        ctx_parts.join("\n\n")
    }

    async fn answer_with_local_model(
        &self,
        chat_id: i64,
        question: &str,
        official_context: &str,
        sources: &[String],
    ) -> Result<String> {
        let history_text = self.render_memory_context(chat_id).await;
        let mut prompt = String::new();
        prompt.push_str(
            "You are a Kaspa-focused assistant integrated in a Telegram bot.\n\
            Rules:\n\
            - Treat user input as either a question, statement, request, or instruction.\n\
            - Respond directly to what the user actually asked or stated.\n\
            - If code is requested, include runnable Rust code and explain it clearly.\n\
            - Use official context below when relevant.\n\
            - Keep continuity with previous chat context if it helps answer accurately.\n\
            - If you are uncertain, state uncertainty clearly and avoid guessing.\n\n",
        );
        if !history_text.is_empty() {
            prompt.push_str("Recent chat context:\n");
            prompt.push_str(&history_text);
            prompt.push_str("\n\n");
        }
        if !official_context.trim().is_empty() {
            prompt.push_str("Official context:\n");
            prompt.push_str(official_context);
            prompt.push_str("\n\n");
        }
        if !sources.is_empty() {
            prompt.push_str("Sources that must be preferred:\n");
            for (i, src) in sources.iter().enumerate() {
                prompt.push_str(&format!("[{}] ", i + 1));
                prompt.push_str(src);
                prompt.push('\n');
            }
            prompt.push('\n');
        }
        prompt.push_str("User input:\n");
        prompt.push_str(question.trim());
        prompt.push_str("\n\nAnswer with concise structure. When making factual claims tied to sources, cite them inline like [1], [2].");

        let payload = LocalModelGenerateRequest {
            model: self.config.local_model.model.clone(),
            prompt,
            stream: false,
        };

        let response = self
            .client
            .post(&self.config.local_model.endpoint)
            .timeout(std::time::Duration::from_secs(
                self.config.local_model.timeout_seconds,
            ))
            .json(&payload)
            .send()
            .await
            .context("Local model endpoint request failed")?;

        if !response.status().is_success() {
            anyhow::bail!("Local model endpoint returned {}", response.status());
        }

        let parsed: LocalModelGenerateResponse = response
            .json()
            .await
            .context("Invalid local model JSON response")?;
        Ok(parsed.response)
    }

    async fn render_memory_context(&self, chat_id: i64) -> String {
        let memory = self.conversation_memory.lock().await;
        let Some(turns) = memory.get(&chat_id) else {
            return String::new();
        };
        turns
            .iter()
            .map(|t| format!("Q: {}\nA: {}", t.question, t.answer))
            .collect::<Vec<_>>()
            .join("\n\n")
    }

    async fn retrieve_local_knowledge_context(
        &self,
        question: &str,
    ) -> Result<(String, Vec<String>)> {
        if !self.config.knowledge.enabled {
            anyhow::bail!("Knowledge retrieval disabled");
        }
        let repos = self.config.knowledge.resolved_repos();
        if repos.is_empty() {
            anyhow::bail!("No knowledge repos configured");
        }
        let any_exists = repos.iter().any(|(_, p, _, _)| p.exists());
        if !any_exists {
            anyhow::bail!("Knowledge repositories not present locally");
        }

        let tokens = Self::query_tokens(question);
        if tokens.is_empty() {
            anyhow::bail!("No retrieval tokens found");
        }

        if let Ok(found) = self.retrieve_local_knowledge_with_bm25(&tokens).await {
            return Ok(found);
        }

        let pattern = tokens
            .iter()
            .map(|t| regex_escape(t))
            .collect::<Vec<_>>()
            .join("|");

        let mut all_ranked: Vec<(String, usize, i64, PathBuf, String)> = Vec::new();
        for (_, repo_path, _, github_base) in &repos {
            if !repo_path.exists() {
                continue;
            }
            let output = tokio::time::timeout(
                std::time::Duration::from_secs(self.config.knowledge.git_timeout_seconds),
                tokio::process::Command::new("git")
                    .arg("-C")
                    .arg(repo_path)
                    .arg("grep")
                    .arg("-n")
                    .arg("-I")
                    .arg("-E")
                    .arg(&pattern)
                    .arg("--")
                    .arg("*.rs")
                    .arg("*.md")
                    .arg("*.toml")
                    .arg("*.py")
                    .arg("*.tex")
                    .output(),
            )
            .await
            .context("Timed out while running git grep for local knowledge")?
            .context("Failed to run git grep for local knowledge")?;

            if !(output.status.success() || output.status.code() == Some(1)) {
                continue;
            }

            let stdout = String::from_utf8_lossy(&output.stdout);
            let head = self.repo_head_short(repo_path).await.unwrap_or_default();
            let ref_head = if head.is_empty() { "master" } else { &head };

            for line in stdout.lines() {
                if let Some((file, line_no, matched)) = parse_grep_line(line) {
                    let score = score_hit(file, matched, &tokens);
                    let file_path = repo_path.join(file);
                    all_ranked.push((
                        file.to_string(),
                        line_no,
                        score,
                        file_path,
                        format!("{}/blob/{}/{}#L{}", github_base, ref_head, file, line_no),
                    ));
                }
            }
        }

        all_ranked.sort_by(|a, b| b.2.cmp(&a.2));
        let mut snippets = Vec::new();
        let mut sources = Vec::new();
        for (file, line_no, _, file_path, src) in all_ranked.into_iter().take(18) {
            if !file_path.exists() {
                continue;
            }
            if let Ok(snippet) = read_file_context(&file_path, line_no, 3, 4) {
                snippets.push(format!("{}:{}\n{}", file, line_no, snippet));
                if !sources.contains(&src) {
                    sources.push(src);
                }
            }
        }

        if snippets.is_empty() {
            anyhow::bail!("No local snippets collected");
        }

        let mut context = snippets.join("\n\n---\n\n");
        let max_chars = self.config.knowledge.max_context_chars.max(500);
        if context.len() > max_chars {
            context.truncate(max_chars);
        }
        Ok((context, sources))
    }

    async fn retrieve_local_knowledge_with_bm25(
        &self,
        tokens: &[String],
    ) -> Result<(String, Vec<String>)> {
        self.ensure_knowledge_index().await?;
        let index_guard = self.knowledge_index.lock().await;
        let Some(index) = index_guard.as_ref() else {
            anyhow::bail!("Knowledge index missing");
        };

        let query_terms = tokens.iter().map(|t| t.to_lowercase()).collect::<Vec<_>>();
        let mut scored = index
            .chunks
            .iter()
            .enumerate()
            .map(|(i, c)| (i, score_bm25(c, index.avg_len, &index.idf, &query_terms)))
            .filter(|(_, s)| *s > 0.0)
            .collect::<Vec<_>>();
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        if scored.is_empty() {
            anyhow::bail!("No BM25 matches");
        }

        let top = scored.into_iter().take(8).collect::<Vec<_>>();
        let mut context_parts = Vec::new();
        let mut sources = Vec::new();
        for (idx, score) in top {
            let chunk = &index.chunks[idx];
            context_parts.push(format!(
                "{}\nScore: {:.3}\n{}",
                chunk.label, score, chunk.text
            ));
            if !sources.contains(&chunk.source_url) {
                sources.push(chunk.source_url.clone());
            }
        }

        let mut context = context_parts.join("\n\n---\n\n");
        let max_chars = self.config.knowledge.max_context_chars.max(500);
        if context.len() > max_chars {
            context.truncate(max_chars);
        }
        Ok((context, sources))
    }

    async fn ensure_knowledge_index(&self) -> Result<()> {
        {
            if self.knowledge_index.lock().await.is_some() {
                return Ok(());
            }
        }

        let repos = self.config.knowledge.resolved_repos();
        let mut inputs: Vec<(PathBuf, String, String)> = Vec::new();
        for (_, path, branch, github_base) in repos {
            if !path.exists() {
                continue;
            }
            let head = self.repo_head_short(&path).await.unwrap_or_default();
            let ref_head = if head.is_empty() { branch } else { head };
            inputs.push((path, ref_head, github_base));
        }

        let index = tokio::task::spawn_blocking(move || build_multi_knowledge_index(&inputs))
            .await
            .context("Knowledge indexing join error")??;
        *self.knowledge_index.lock().await = Some(index);
        Ok(())
    }

    async fn repo_head_short(&self, repo_path: &Path) -> Result<String> {
        let output = tokio::time::timeout(
            std::time::Duration::from_secs(self.config.knowledge.git_timeout_seconds),
            tokio::process::Command::new("git")
                .arg("-C")
                .arg(repo_path)
                .arg("rev-parse")
                .arg("--short")
                .arg("HEAD")
                .output(),
        )
        .await
        .context("Timed out while reading knowledge repo HEAD")?
        .context("Failed to read knowledge repo HEAD")?;
        if !output.status.success() {
            anyhow::bail!("git rev-parse failed");
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    async fn local_model_health(&self) -> String {
        let endpoint = reqwest::Url::parse(&self.config.local_model.endpoint);
        let Ok(mut url) = endpoint else {
            return "invalid_endpoint".to_string();
        };
        url.set_path("/api/tags");
        url.set_query(None);

        match self
            .client
            .get(url)
            .timeout(std::time::Duration::from_secs(2))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => "ok".to_string(),
            Ok(resp) => format!("http_{}", resp.status().as_u16()),
            Err(_) => "down".to_string(),
        }
    }

    fn query_tokens(question: &str) -> Vec<String> {
        question
            .split(|c: char| !c.is_alphanumeric() && c != '_' && c != '/')
            .filter_map(|s| {
                let t = s.trim().to_lowercase();
                if t.len() >= 3 {
                    Some(t)
                } else {
                    None
                }
            })
            .take(12)
            .collect()
    }

    async fn fetch_contributors_context(&self) -> Result<String> {
        let url = "https://api.github.com/repos/kaspanet/rusty-kaspa/contributors?per_page=10";
        let response = self
            .client
            .get(url)
            .header("User-Agent", "kaspa-telegram-bot")
            .send()
            .await
            .context("Failed to fetch contributors")?;
        if !response.status().is_success() {
            anyhow::bail!("Contributor endpoint status {}", response.status());
        }
        let contributors = response
            .json::<Vec<GitHubContributor>>()
            .await
            .context("Failed to parse contributors")?;
        if contributors.is_empty() {
            anyhow::bail!("No contributors returned");
        }
        let top = contributors
            .into_iter()
            .take(10)
            .map(|c| format!("@{} ({} commits)", c.login, c.contributions))
            .collect::<Vec<_>>()
            .join("\n");
        Ok(format!(
            "Top contributors for kaspanet/rusty-kaspa:\n{}\nSource: https://github.com/kaspanet/rusty-kaspa",
            top
        ))
    }

    async fn fetch_repo_context(&self, question: &str) -> Result<String> {
        let tokens = question
            .split(|c: char| !c.is_alphanumeric())
            .filter(|w| w.len() >= 3)
            .take(8)
            .collect::<Vec<_>>();
        if tokens.is_empty() {
            anyhow::bail!("No search tokens");
        }

        let query = format!("org:kaspanet {}", tokens.join(" "));
        let mut url = reqwest::Url::parse("https://api.github.com/search/repositories")
            .context("Invalid search URL")?;
        url.query_pairs_mut()
            .append_pair("q", &query)
            .append_pair("per_page", "5")
            .append_pair("sort", "updated")
            .append_pair("order", "desc");

        let response = self
            .client
            .get(url)
            .header("User-Agent", "kaspa-telegram-bot")
            .send()
            .await
            .context("Failed to fetch repository search data")?;
        if !response.status().is_success() {
            anyhow::bail!("Repository search status {}", response.status());
        }
        let payload = response
            .json::<GitHubRepoSearchResponse>()
            .await
            .context("Failed to parse repository search JSON")?;
        if payload.items.is_empty() {
            anyhow::bail!("No repository search matches");
        }
        let lines = payload
            .items
            .into_iter()
            .take(3)
            .map(|repo| {
                let desc = repo
                    .description
                    .unwrap_or_else(|| "No description provided.".to_string());
                format!("- {}: {} ({})", repo.full_name, desc, repo.html_url)
            })
            .collect::<Vec<_>>()
            .join("\n");
        Ok(format!("Relevant official Kaspa repositories:\n{}", lines))
    }

    async fn fetch_bridge_contributors_context(&self) -> Result<String> {
        let url = "https://api.github.com/repos/kaspanet/rusty-kaspa/commits?per_page=100";
        let response = self
            .client
            .get(url)
            .header("User-Agent", "kaspa-telegram-bot")
            .send()
            .await
            .context("Failed to fetch recent commits")?;
        if !response.status().is_success() {
            anyhow::bail!("Commit list endpoint status {}", response.status());
        }
        let commits = response
            .json::<Vec<GitHubCommitResponse>>()
            .await
            .context("Failed to parse commit list JSON")?;
        if commits.is_empty() {
            anyhow::bail!("No commits returned");
        }

        let mut counts: HashMap<String, u64> = HashMap::new();
        let mut examples: Vec<String> = Vec::new();

        for c in commits {
            let msg = c.commit.message.to_lowercase();
            if msg.contains("bridge") {
                let key = c
                    .author
                    .as_ref()
                    .map(|a| format!("@{}", a.login))
                    .unwrap_or_else(|| c.commit.author.name.clone());
                *counts.entry(key).or_insert(0) += 1;
                if examples.len() < 4 {
                    let title = c.commit.message.lines().next().unwrap_or("commit");
                    examples.push(format!(
                        "- {}: {} (https://github.com/kaspanet/rusty-kaspa/commit/{})",
                        c.sha, title, c.sha
                    ));
                }
            }
        }

        if counts.is_empty() {
            anyhow::bail!("No recent bridge-related commits found");
        }

        let mut ranking = counts.into_iter().collect::<Vec<_>>();
        ranking.sort_by(|a, b| b.1.cmp(&a.1));
        let top = ranking
            .into_iter()
            .take(6)
            .map(|(name, n)| format!("- {} ({} bridge-tagged commits in recent sample)", name, n))
            .collect::<Vec<_>>()
            .join("\n");

        Ok(format!(
            "Bridge-related attribution from recent commits in kaspanet/rusty-kaspa (sample of latest 100 commits):\n{}\n\nExample bridge-related commits:\n{}\n\nSource: https://github.com/kaspanet/rusty-kaspa/commits",
            top,
            examples.join("\n")
        ))
    }

    async fn fetch_commit_context(&self, sha: &str) -> Result<String> {
        let url = format!(
            "https://api.github.com/repos/kaspanet/rusty-kaspa/commits/{}",
            sha
        );
        let response = self
            .client
            .get(&url)
            .header("User-Agent", "kaspa-telegram-bot")
            .send()
            .await
            .with_context(|| format!("Failed to fetch commit {}", sha))?;
        if !response.status().is_success() {
            anyhow::bail!("Commit endpoint status {}", response.status());
        }
        let commit = response
            .json::<GitHubCommitResponse>()
            .await
            .context("Failed to parse commit JSON")?;

        let display_author = commit
            .author
            .as_ref()
            .map(|a| format!("@{}", a.login))
            .unwrap_or_else(|| commit.commit.author.name.clone());
        let title = commit.commit.message.lines().next().unwrap_or("commit");
        let bridge_touched = commit
            .files
            .as_ref()
            .map(|files| {
                files.iter().any(|f| {
                    let lower = f.filename.to_lowercase();
                    lower.contains("bridge") || lower.contains("grpc") || lower.contains("telegram")
                })
            })
            .unwrap_or(false);
        Ok(format!(
            "Commit lookup:\n- SHA: {}\n- Author: {}\n- Title: {}\n- Bridge-related file touched (heuristic): {}\n- URL: https://github.com/kaspanet/rusty-kaspa/commit/{}",
            commit.sha, display_author, title, bridge_touched, commit.sha
        ))
    }

    fn extract_commit_sha(input: &str) -> Option<String> {
        input
            .split(|c: char| !c.is_ascii_hexdigit())
            .find(|part| part.len() == 40)
            .map(|s| s.to_lowercase())
    }

    async fn answer_kaspa_contributors(&self) -> String {
        match self.fetch_contributors_context().await {
            Ok(context) => context,
            Err(_) => {
                "I could not fetch live contributor data right now. Please try again in a few seconds."
                    .to_string()
            }
        }
    }

    fn generate_builtin_answer(&self, question: &str) -> String {
        let q = question.trim();
        if q.is_empty() {
            return "Please send a Kaspa-related prompt (question, statement, or request)."
                .to_string();
        }

        let lq = q.to_lowercase();
        if lq.contains("write me code") || (lq.contains("code") && lq.contains("kaspa")) {
            return "Here is a Rust example for your bot that validates a Kaspa address and converts KAS to sompi:\n\n```rust\nuse kaspa_addresses::Address;\n\nconst SOMPI_PER_KAS: u64 = 100_000_000;\n\nfn parse_kas_to_sompi(amount_kas: f64) -> Result<u64, String> {\n    if !amount_kas.is_finite() || amount_kas <= 0.0 {\n        return Err(\"amount must be a positive number\".to_string());\n    }\n    let sompi = (amount_kas * SOMPI_PER_KAS as f64).round();\n    if sompi > u64::MAX as f64 {\n        return Err(\"amount is too large\".to_string());\n    }\n    Ok(sompi as u64)\n}\n\nfn validate_kaspa_address(addr: &str) -> bool {\n    Address::try_from(addr).is_ok()\n}\n\nfn main() {\n    let to = \"kaspa:qq3tqr9f0z6t6zwcrjkk8krwwltazcl0s4gvelvakvqmj9essyq4kaksa3v0m\";\n    let amount_kas = 1.25;\n\n    if !validate_kaspa_address(to) {\n        eprintln!(\"Invalid Kaspa address\");\n        return;\n    }\n\n    match parse_kas_to_sompi(amount_kas) {\n        Ok(sompi) => println!(\"Send {} KAS ({} sompi) to {}\", amount_kas, sompi, to),\n        Err(e) => eprintln!(\"Invalid amount: {}\", e),\n    }\n}\n```\n\nWhat it does:\n1) Checks destination address format.\n2) Converts KAS to sompi safely.\n3) Prints a ready-to-send normalized amount.\n\nIf you want, I can generate the next step too: building and signing a full transaction flow.".to_string();
        }
        if lq.contains("what is kaspa") {
            return "Kaspa is a proof-of-work Layer-1 network using the GHOSTDAG protocol, designed for high block rates and fast confirmation visibility.".to_string();
        }
        if lq.contains("who made kaspa")
            || lq.contains("who created kaspa")
            || lq.contains("who founded kaspa")
        {
            return "Kaspa was initiated by Dr. Yonatan Sompolinsky, and it is developed as an open-source project by a broader set of contributors and maintainers.".to_string();
        }
        if lq.contains("ghostdag") {
            return "GHOSTDAG is Kaspa's blockDAG consensus ordering method. Instead of a single-chain winner, it orders many parallel blocks while preserving security assumptions of PoW.".to_string();
        }
        if lq.contains("daa") || lq.contains("daa score") {
            return "DAA score is Kaspa's difficulty-adjusted progression metric used as a stable confirmation reference. In this bot, confirmation checks compare virtual DAA score against transaction/block DAA score depth.".to_string();
        }
        if lq.contains("confirm") && lq.contains("block") {
            return "For this bot, a block reward is considered only after policy checks (blue status + DAA depth + reward existence checks). That reduces false notifications from non-final rewards.".to_string();
        }
        if lq.contains("wallet") && lq.contains("import") {
            return "Use `/wallet importpk <64-hex-private-key>` to load a local session credential, `/wallet status` to verify loaded state, and `/wallet clear` to remove it.".to_string();
        }
        if lq.contains("/send") || (lq.contains("send") && lq.contains("kas")) {
            return "Use `/send` and follow prompts: source address, destination address, then amount. You can cancel at any time with `/cancel`.".to_string();
        }
        if lq.contains("mining") && (lq.contains("reward") || lq.contains("solo")) {
            return "Solo reward notifications are tied to tracked addresses and only emitted after configured confirmation policy passes. Check `/rewards` for your recent reward history.".to_string();
        }
        if lq.contains("fee") {
            return "Transaction fees depend on transaction mass and selected inputs/outputs. The safest way is to check current node fee estimation and final signed transaction mass at send-time.".to_string();
        }

        format!(
            "I could not find an exact official-source match yet for: \"{}\".\n\nBest next step: send one specific Kaspa prompt, for example:\n- \"Which Kaspa repo handles full node logic?\"\n- \"Explain what GHOSTDAG does in Kaspa\"\n- \"Show how to use /send in this bot\"\n\nI will respond directly from official Kaspa sources or built-in bot logic.",
            q
        )
    }

    async fn answer_from_kaspanet_sources(&self, question: &str) -> Option<String> {
        let Ok(context) = self.fetch_repo_context(question).await else {
            return None;
        };
        let base = format!(
            "{}\n\nIf you want, ask a follow-up about one repo and I will give a more direct explanation.",
            context
        );
        let sources = Self::extract_sources_from_context(&base);
        Some(Self::ensure_sources_section(&base, &sources, &[]))
    }

    fn extract_sources_from_context(context: &str) -> Vec<String> {
        let mut sources = Vec::new();
        for token in context.split_whitespace() {
            if token.starts_with("http://") || token.starts_with("https://") {
                let cleaned = token
                    .trim_end_matches(|c: char| [',', '.', ')', ']', ';'].contains(&c))
                    .to_string();
                if !sources.contains(&cleaned) {
                    sources.push(cleaned);
                }
            }
        }
        sources
    }

    fn ensure_sources_section(answer: &str, sources: &[String], cited_indexes: &[usize]) -> String {
        if sources.is_empty() || answer.contains("Sources:") {
            return answer.to_string();
        }
        let filtered = if cited_indexes.is_empty() {
            sources.to_vec()
        } else {
            cited_indexes
                .iter()
                .filter_map(|idx| sources.get(*idx).cloned())
                .collect::<Vec<_>>()
        };
        if filtered.is_empty() {
            return answer.to_string();
        }
        let mut out = answer.to_string();
        out.push_str("\n\nSources:\n");
        for source in &filtered {
            out.push_str("- ");
            out.push_str(source);
            out.push('\n');
        }
        out.trim_end().to_string()
    }

    fn extract_cited_source_indexes(answer: &str, sources_len: usize) -> Vec<usize> {
        let bytes = answer.as_bytes();
        let mut out = Vec::new();
        let mut i = 0;
        while i + 2 < bytes.len() {
            if bytes[i] == b'[' {
                let mut j = i + 1;
                let mut n: usize = 0;
                let mut has_digit = false;
                while j < bytes.len() && bytes[j].is_ascii_digit() {
                    has_digit = true;
                    n = n
                        .saturating_mul(10)
                        .saturating_add((bytes[j] - b'0') as usize);
                    j += 1;
                }
                if has_digit && j < bytes.len() && bytes[j] == b']' {
                    if n >= 1 && n <= sources_len {
                        let idx = n - 1;
                        if !out.contains(&idx) {
                            out.push(idx);
                        }
                    }
                    i = j + 1;
                    continue;
                }
            }
            i += 1;
        }
        out
    }
}

#[derive(Debug, Clone)]
struct ConversationTurn {
    question: String,
    answer: String,
}

#[derive(Debug, Deserialize)]
struct GitHubContributor {
    login: String,
    contributions: u64,
}

#[derive(Debug, Deserialize)]
struct GitHubRepoSearchResponse {
    items: Vec<GitHubRepoSearchItem>,
}

#[derive(Debug, Deserialize)]
struct GitHubRepoSearchItem {
    full_name: String,
    html_url: String,
    description: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GitHubCommitResponse {
    sha: String,
    author: Option<GitHubUser>,
    commit: GitInnerCommit,
    files: Option<Vec<GitHubCommitFile>>,
}

#[derive(Debug, Deserialize)]
struct GitInnerCommit {
    author: GitInnerAuthor,
    message: String,
}

#[derive(Debug, Deserialize)]
struct GitInnerAuthor {
    name: String,
}

#[derive(Debug, Deserialize)]
struct GitHubUser {
    login: String,
}

#[derive(Debug, Deserialize)]
struct GitHubCommitFile {
    filename: String,
}

#[derive(Debug, Serialize)]
struct LocalModelGenerateRequest {
    model: String,
    prompt: String,
    stream: bool,
}

#[derive(Debug, Deserialize)]
struct LocalModelGenerateResponse {
    response: String,
}

#[derive(Debug, Clone)]
struct KnowledgeIndex {
    chunks: Vec<KnowledgeChunk>,
    idf: HashMap<String, f64>,
    avg_len: f64,
}

#[derive(Debug, Clone)]
struct KnowledgeChunk {
    label: String,
    text: String,
    source_url: String,
    token_freq: HashMap<String, usize>,
    len: usize,
}

fn parse_grep_line(line: &str) -> Option<(&str, usize, &str)> {
    let mut parts = line.splitn(3, ':');
    let file = parts.next()?;
    let line_no_str = parts.next()?;
    let matched = parts.next().unwrap_or_default();
    let line_no = line_no_str.parse::<usize>().ok()?;
    Some((file, line_no, matched))
}

fn read_file_context(
    path: &Path,
    center_line: usize,
    before: usize,
    after: usize,
) -> Result<String> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read context file {}", path.display()))?;
    let lines = content.lines().collect::<Vec<_>>();
    if lines.is_empty() {
        return Ok(String::new());
    }
    let start = center_line.saturating_sub(before + 1);
    let end = (center_line + after).min(lines.len());
    let mut out = String::new();
    for (idx, line) in lines[start..end].iter().enumerate() {
        let actual = start + idx + 1;
        out.push_str(&format!("L{}: {}\n", actual, line));
    }
    Ok(out.trim_end().to_string())
}

fn regex_escape(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '.' | '^' | '$' | '|' | '(' | ')' | '[' | ']' | '{' | '}' | '*' | '+' | '?' | '\\' => {
                out.push('\\');
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }
    out
}

fn score_hit(file: &str, matched: &str, tokens: &[String]) -> i64 {
    let file_lower = file.to_lowercase();
    let matched_lower = matched.to_lowercase();
    let mut score: i64 = 0;
    for t in tokens {
        if file_lower.contains(t) {
            score += 8;
        }
        if matched_lower.contains(t) {
            score += 5;
        }
    }
    if file_lower.contains("bridge") {
        score += 6;
    }
    if file_lower.contains("wallet") || file_lower.contains("rpc") || file_lower.contains("docs") {
        score += 3;
    }
    score
}

fn build_multi_knowledge_index(inputs: &[(PathBuf, String, String)]) -> Result<KnowledgeIndex> {
    let mut chunks = Vec::new();
    for (repo_path, head, github_base) in inputs {
        let mut files = Vec::new();
        collect_indexable_files(repo_path, &mut files)?;
        for file in files {
            let rel = match file.strip_prefix(repo_path) {
                Ok(v) => v.to_string_lossy().replace('\\', "/"),
                Err(_) => continue,
            };
            let content = match std::fs::read_to_string(&file) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let lines = content.lines().collect::<Vec<_>>();
            if lines.is_empty() {
                continue;
            }
            let (chunk_size, stride) = if rel.ends_with(".rs") {
                (40usize, 24usize)
            } else {
                (32usize, 20usize)
            };
            let mut start = 0usize;
            while start < lines.len() {
                let end = (start + chunk_size).min(lines.len());
                let text = lines[start..end].join("\n");
                let toks = tokenize_text(&text);
                if !toks.is_empty() {
                    let mut tf = HashMap::<String, usize>::new();
                    for t in toks {
                        *tf.entry(t).or_insert(0) += 1;
                    }
                    let line_start = start + 1;
                    let label = format!("{}:{}-{}", rel, line_start, end);
                    let source_url =
                        format!("{}/blob/{}/{}#L{}", github_base, head, rel, line_start);
                    let len = tf.values().sum::<usize>();
                    chunks.push(KnowledgeChunk {
                        label,
                        text,
                        source_url,
                        token_freq: tf,
                        len,
                    });
                }
                if end == lines.len() {
                    break;
                }
                start += stride;
            }
        }
    }

    if chunks.is_empty() {
        anyhow::bail!("No knowledge chunks built");
    }

    let n_docs = chunks.len() as f64;
    let mut df = HashMap::<String, usize>::new();
    for c in &chunks {
        let keys = c.token_freq.keys().cloned().collect::<Vec<_>>();
        for k in keys {
            *df.entry(k).or_insert(0) += 1;
        }
    }
    let idf = df
        .into_iter()
        .map(|(term, d)| {
            let d = d as f64;
            let val = ((n_docs - d + 0.5) / (d + 0.5) + 1.0).ln();
            (term, val)
        })
        .collect::<HashMap<_, _>>();
    let avg_len = (chunks.iter().map(|c| c.len).sum::<usize>() as f64 / n_docs).max(1.0);
    Ok(KnowledgeIndex {
        chunks,
        idf,
        avg_len,
    })
}

fn collect_indexable_files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    for entry in
        std::fs::read_dir(dir).with_context(|| format!("Failed reading {}", dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default();
            if name == ".git" || name == "target" || name == "node_modules" {
                continue;
            }
            collect_indexable_files(&path, out)?;
        } else if path.is_file() {
            let keep = path
                .extension()
                .and_then(|e| e.to_str())
                .map(|ext| matches!(ext, "rs" | "md" | "toml" | "py" | "tex"))
                .unwrap_or(false);
            if keep {
                out.push(path);
            }
        }
    }
    Ok(())
}

fn tokenize_text(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric() && c != '_' && c != '/')
        .filter_map(|s| {
            let t = s.trim().to_lowercase();
            if t.len() >= 2 {
                Some(t)
            } else {
                None
            }
        })
        .collect()
}

fn score_bm25(
    chunk: &KnowledgeChunk,
    avg_len: f64,
    idf: &HashMap<String, f64>,
    query_terms: &[String],
) -> f64 {
    let k1 = 1.5;
    let b = 0.75;
    let doc_len = chunk.len as f64;
    let mut score = 0.0;
    for term in query_terms {
        let tf = *chunk.token_freq.get(term).unwrap_or(&0) as f64;
        if tf <= 0.0 {
            continue;
        }
        let term_idf = *idf.get(term).unwrap_or(&0.0);
        let denom = tf + k1 * (1.0 - b + b * (doc_len / avg_len));
        score += term_idf * ((tf * (k1 + 1.0)) / denom);
    }
    score
}
