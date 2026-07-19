//! High-level Codex app-server client: initialize/account/model/rate-limit
//! reads plus one ephemeral read-only thread and turn, using the stable
//! sandbox shape confirmed in CP1 (`sandbox: "read-only"`, not a map).

use std::collections::HashSet;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot};

use crate::journal::{JournalEvent, JournalWriter};
use crate::jsonl::{JsonlClient, JsonlError, RequestDelivery};
use crate::process::ChildProcess;
use crate::state::{ApprovalDecision, ApprovalSource, InternalEvent, SessionState, StateError};

pub const REQUIRED_MODEL: &str = "gpt-5.3-codex-spark";
const MAX_SEEN_APPROVALS: usize = 256;
const MAX_SEEN_APPROVAL_BYTES: usize = 16 * 1024;
const MAX_MODEL_LIST_PAGES: usize = 32;
const MAX_MODEL_CURSOR_LEN: usize = 4096;

/// The child controls both JSON-RPC request ids and approval identifiers.
/// Keep only fixed-size opaque keys and bound the duplicate detector so a
/// hostile server cannot turn an approval stream into retained memory.
#[derive(Default)]
struct SeenApprovals {
    keys: HashSet<String>,
    bytes: usize,
}

impl SeenApprovals {
    fn insert(&mut self, key: String) -> Result<bool, ClientError> {
        if self.keys.contains(&key) {
            return Ok(false);
        }
        if self.keys.len() >= MAX_SEEN_APPROVALS
            || self.bytes.saturating_add(key.len()) > MAX_SEEN_APPROVAL_BYTES
        {
            return Err(ClientError::SessionPoisoned);
        }
        self.bytes = self.bytes.saturating_add(key.len());
        Ok(self.keys.insert(key))
    }

    fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error(transparent)]
    Jsonl(#[from] JsonlError),
    #[error(transparent)]
    State(#[from] StateError),
    #[error(
        "app-server substituted model (class={class}, hash={hash}) instead of required {required}"
    )]
    FallbackModel {
        /// Remote model names are never retained.  They can contain arbitrary
        /// child-controlled text, so diagnostics carry only a stable class and
        /// a bounded fingerprint.
        class: &'static str,
        hash: String,
        required: &'static str,
    },
    #[error("thread/start response missing thread.id")]
    MissingThreadId,
    #[error("turn/completed notification missing turn.status field")]
    MissingTurnStatus,
    #[error("turn/start response missing turn.id")]
    MissingTurnId,
    #[error("session state was poisoned by a protocol desync")]
    SessionPoisoned,
    #[error("server approval request missing a string or signed-integer id")]
    MissingServerRequestId,
    #[error("unknown server request class; session poisoned")]
    UnknownServerRequest,
    #[error("duplicate approval request {request_key}; session poisoned")]
    DuplicateApproval { request_key: String },
    #[error("unexpected response while waiting for terminal turn notification")]
    UnexpectedResponseWhileWaiting,
    #[error("protocol desync after an approval boundary; restart denied fail-closed")]
    UnresolvedApprovalRestart,
    #[error("protocol state is ambiguous after non-idempotent {method}; automatic replay denied")]
    AmbiguousNonIdempotent { method: &'static str },
    #[error("app-server account is not authenticated through the ChatGPT route")]
    ChatGptAuthRequired,
    #[error("app-server rate-limit response has no remaining quota")]
    QuotaUnavailable,
    #[error("runtime turn deadline elapsed; execution cancelled")]
    TurnDeadlineExceeded,
    #[error("app-server requested ChatGPT token refresh but the runtime owner has no bounded refresh response")]
    AuthTokensRefreshUnavailable,
}

impl ClientError {
    pub fn class(&self) -> &'static str {
        match self {
            Self::FallbackModel { .. } => "required_model_unavailable",
            Self::ChatGptAuthRequired => "chatgpt_auth_required",
            Self::QuotaUnavailable => "quota_unavailable",
            Self::AuthTokensRefreshUnavailable => "auth_refresh_unavailable",
            _ => "protocol_failure",
        }
    }

    /// Whether this error is a JSONL protocol desync (oversized/malformed
    /// frame or an unexpected response id) that poisoned the session and may
    /// be worth a single controlled app-server restart. Other failures
    /// (fallback model, invalid state transitions, timeouts) are not
    /// automatically retried.
    pub fn is_recoverable_desync(&self) -> bool {
        matches!(self, ClientError::Jsonl(error) if error.is_desync())
    }
}

fn remote_value_hash(value: &str) -> String {
    // This is a diagnostic correlation token, not a security primitive.  It
    // deliberately hashes the complete remote value while retaining only a
    // fixed-width hexadecimal representation at every error boundary.
    use std::hash::{Hash, Hasher};

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn fallback_model(class: &'static str, observed: &str) -> ClientError {
    ClientError::FallbackModel {
        class,
        hash: remote_value_hash(observed),
        required: REQUIRED_MODEL,
    }
}

fn model_list_contains_required_model(models: &Value) -> bool {
    models
        .get("data")
        .or_else(|| models.get("models"))
        .and_then(Value::as_array)
        .is_some_and(|models| {
            models.iter().any(|entry| {
                ["id", "model"]
                    .iter()
                    .any(|field| entry.get(field).and_then(Value::as_str) == Some(REQUIRED_MODEL))
            })
        })
}

/// Remote quota responses are diagnostics, not audit payloads.  Keep only
/// the admission fact and a fixed-width correlation hash so arbitrary child
/// fields (including canaries under innocent keys) never cross into SQLite.
pub fn journal_rate_limit_snapshot(snapshot: &Value) -> Value {
    let encoded = serde_json::to_string(snapshot).unwrap_or_default();
    json!({
        "class": "rate_limit_snapshot",
        "quota_available": quota_available(snapshot),
        "hash": remote_value_hash(&encoded),
    })
}

#[derive(Debug, Clone)]
pub enum ApprovalPolicy {
    Deny,
    AllowForTests,
    /// The sole runtime owner relays a genuine server request to an
    /// authenticated adapter.  The original JSON-RPC id never leaves this
    /// client; the adapter receives only an opaque pending handle.
    External {
        pending: mpsc::Sender<PendingApproval>,
        timeout: Duration,
        /// Receipt is installed only by the runtime owner.  Recording here,
        /// before the request is handed to HTTP, makes a crash at the pending
        /// boundary recoverable instead of relying on end-of-flow event
        /// projection.
        receipt: Option<ApprovalReceipt>,
    },
}

#[derive(Debug, Clone)]
pub struct ApprovalReceipt {
    pub journal: JournalWriter,
    pub execution_id: String,
}

/// The protocol owner may relay a refresh request to an authenticated
/// authority, but it never obtains credentials from the environment or logs
/// them.  The default is deliberately unavailable and therefore fail-closed.
#[derive(Debug, Clone)]
pub enum AuthRefreshPolicy {
    Unavailable,
    External {
        pending: mpsc::Sender<PendingAuthRefresh>,
        timeout: Duration,
    },
}

/// A decision channel for one real app-server request.  It is deliberately
/// one-shot: duplicate HTTP decisions, disconnects and owner shutdown cannot
/// result in two JSON-RPC responses for the same request id.
#[derive(Debug)]
pub struct PendingApproval {
    pub request_key: String,
    pub method: String,
    pub descriptor: ApprovalDescriptor,
    /// A permission grant is permitted only when the exact requested profile
    /// passed the bounded 0.144.3-shaped validation below.
    pub allow_permitted: bool,
    /// The owner, not a detached client future, owns expiry.  This lets the
    /// same cancellation command deny, interrupt, await terminal completion,
    /// and reap the process group in one ordered path.
    pub deadline: Duration,
    pub decision: oneshot::Sender<ApprovalCommand>,
}

/// Bounded, schema-aware context shown only to the authenticated local
/// approval authority. It contains no original request id or opaque child
/// id. Fields that are part of the action an Allow grants are copied without
/// normalization. In particular, an allow-listed patch carries its exact
/// content/diff bytes; a patch that cannot fit through the bounded review
/// path is deny-only.
#[derive(Debug, Clone, Serialize)]
pub struct ApprovalDescriptor {
    pub kind: &'static str,
    /// False means a bounded/redacted summary could not show the whole scope,
    /// so the authenticated authority may only deny the request.
    pub reviewable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    /// `execCommandApproval` grants an argv-like array.  Keep it separate
    /// from the shell-command form above: joining arguments with whitespace
    /// would change what the local authority reviews.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command_arguments: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    /// The target environment changes where an accepted command or
    /// permission profile applies, so it is part of the reviewed authority.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub environment_id: Option<String>,
    /// A managed-network request has authority beyond the shell text alone.
    /// Its protocol and host are shown exactly or the request is deny-only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network_approval: Option<ApprovalNetworkApproval>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub file_changes: Vec<ApprovalFileChange>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requested_permissions: Option<RequestedPermissions>,
    /// The exact bounded profile that an Allow will return on the original
    /// request. It is exposed only when every string is safe to show without
    /// redaction or normalization; otherwise `reviewable` is false and no
    /// Allow is delivered.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requested_permission_profile: Option<Value>,
    /// These response fields are fixed by this owner for permission grants.
    /// They are still shown so the authority sees the scope of its Allow.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permission_grant_scope: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strict_auto_review: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ApprovalFileChange {
    pub path: String,
    pub operation: String,
    /// An update can move the source map key to a different destination. The
    /// destination is part of the approved write scope, so it must be shown
    /// verbatim (or the request is deny-only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub move_path: Option<String>,
    /// Required for add/delete. This is deliberately not sanitized: changing
    /// any byte would make a user review a different write than Allow grants.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    /// Required for update. This is deliberately not sanitized for the same
    /// reason as add/delete content above.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unified_diff: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ApprovalNetworkApproval {
    pub host: String,
    pub protocol: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct RequestedPermissions {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub file_system: Vec<RequestedFileSystemPermission>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network_enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub glob_scan_max_depth: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RequestedFileSystemPermission {
    pub access: String,
    pub kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pattern: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub special_kind: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subpath: Option<String>,
}

/// The authority-bearing fields of every server approval request supported by
/// the pinned 0.144.3 protocol schema.  This is deliberately explicit rather
/// than inferred from a generic JSON object: an added field must be placed in
/// one of these buckets before it can become approvable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApprovalSchemaMatrixEntry {
    pub method: &'static str,
    /// Every top-level field declared by the pinned 0.144.3 request schema.
    /// An extension is deny-only, even when a generated schema would accept
    /// it, until it has been classified in this matrix. This prevents a newer
    /// authority-bearing field from being silently ignored by an old owner.
    pub schema_fields: &'static [&'static str],
    /// Fields copied losslessly into [`ApprovalDescriptor`] when Allow is
    /// possible. `reason` is shown as decision context even though it does
    /// not itself grant authority.
    pub descriptor_fields: &'static [&'static str],
    /// Fields whose schema offers a broader persistent authority than this
    /// owner ever returns. If present, this owner refuses Allow instead of
    /// silently dropping the proposal.
    pub deny_allow_if_present: &'static [&'static str],
    /// Authority granted by this response but not carried in the request.
    /// Such a variant is deny-only until a bounded, exact correlation path
    /// delivers this payload to the descriptor.
    pub unreviewable_authority: Option<&'static str>,
    /// Routing/correlation fields and explicitly best-effort display metadata
    /// that do not change the action of this owner's one-request response.
    pub non_authority_fields: &'static [&'static str],
    /// False when the generated request's response grants authority whose
    /// exact payload is carried only by a separate notification. The request
    /// remains describable for diagnostics, but cannot become actionable.
    pub allow_supported: bool,
}

pub const APPROVAL_SCHEMA_MATRIX: &[ApprovalSchemaMatrixEntry] = &[
    ApprovalSchemaMatrixEntry {
        method: "item/commandExecution/requestApproval",
        schema_fields: &[
            "turnId",
            "approvalId",
            "threadId",
            "command",
            "commandActions",
            "cwd",
            "environmentId",
            "itemId",
            "networkApprovalContext",
            "proposedExecpolicyAmendment",
            "proposedNetworkPolicyAmendments",
            "reason",
            "startedAtMs",
        ],
        descriptor_fields: &[
            "command",
            "cwd",
            "environmentId",
            "networkApprovalContext.host",
            "networkApprovalContext.protocol",
            "reason",
        ],
        deny_allow_if_present: &[
            "proposedExecpolicyAmendment",
            "proposedNetworkPolicyAmendments",
        ],
        unreviewable_authority: None,
        non_authority_fields: &[
            "approvalId",
            "itemId",
            "threadId",
            "turnId",
            "startedAtMs",
            "commandActions",
        ],
        allow_supported: true,
    },
    ApprovalSchemaMatrixEntry {
        method: "execCommandApproval",
        schema_fields: &[
            "approvalId",
            "callId",
            "command",
            "conversationId",
            "cwd",
            "parsedCmd",
            "reason",
        ],
        descriptor_fields: &["command[]", "cwd", "reason"],
        deny_allow_if_present: &[],
        unreviewable_authority: None,
        non_authority_fields: &["approvalId", "callId", "conversationId", "parsedCmd"],
        allow_supported: true,
    },
    ApprovalSchemaMatrixEntry {
        method: "applyPatchApproval",
        schema_fields: &[
            "callId",
            "conversationId",
            "fileChanges",
            "grantRoot",
            "reason",
        ],
        descriptor_fields: &[
            "grantRoot",
            "fileChanges.<path>.type",
            "fileChanges.<path>.content",
            "fileChanges.<path>.unified_diff",
            "fileChanges.<path>.move_path",
            "reason",
        ],
        deny_allow_if_present: &[],
        unreviewable_authority: None,
        non_authority_fields: &["callId", "conversationId"],
        allow_supported: true,
    },
    ApprovalSchemaMatrixEntry {
        method: "item/fileChange/requestApproval",
        schema_fields: &[
            "grantRoot",
            "itemId",
            "reason",
            "startedAtMs",
            "threadId",
            "turnId",
        ],
        descriptor_fields: &["grantRoot", "reason"],
        deny_allow_if_present: &[],
        unreviewable_authority: Some("correlated FileChangeThreadItem.changes"),
        non_authority_fields: &["itemId", "threadId", "turnId", "startedAtMs"],
        allow_supported: false,
    },
    ApprovalSchemaMatrixEntry {
        method: "item/permissions/requestApproval",
        schema_fields: &[
            "cwd",
            "environmentId",
            "itemId",
            "permissions",
            "reason",
            "startedAtMs",
            "threadId",
            "turnId",
        ],
        descriptor_fields: &[
            "cwd",
            "environmentId",
            "permissions.fileSystem.entries",
            "permissions.fileSystem.globScanMaxDepth",
            "permissions.fileSystem.read",
            "permissions.fileSystem.write",
            "permissions.network.enabled",
            "response.scope",
            "response.strictAutoReview",
            "reason",
        ],
        deny_allow_if_present: &[],
        unreviewable_authority: None,
        non_authority_fields: &["itemId", "threadId", "turnId", "startedAtMs"],
        allow_supported: true,
    },
];

fn approval_schema_variant(method: &str) -> Option<&'static ApprovalSchemaMatrixEntry> {
    APPROVAL_SCHEMA_MATRIX
        .iter()
        .find(|variant| variant.method == method)
}

fn approval_route_matches(
    method: &str,
    params: &Value,
    active_thread_id: Option<&str>,
    active_turn_id: Option<&str>,
) -> bool {
    let Some(schema) = approval_schema_variant(method) else {
        return false;
    };
    let Some(thread_id) = active_thread_id else {
        return false;
    };
    let Some(turn_id) = active_turn_id else {
        return false;
    };

    (!schema.schema_fields.contains(&"threadId")
        || params.get("threadId").and_then(Value::as_str) == Some(thread_id))
        && (!schema.schema_fields.contains(&"turnId")
            || params.get("turnId").and_then(Value::as_str) == Some(turn_id))
        && (!schema.schema_fields.contains(&"conversationId")
            || params.get("conversationId").and_then(Value::as_str) == Some(thread_id))
}

fn terminal_route_matches(
    params: &Value,
    active_thread_id: Option<&str>,
    active_turn_id: Option<&str>,
) -> bool {
    params.get("threadId").and_then(Value::as_str) == active_thread_id
        && params
            .get("turn")
            .and_then(|turn| turn.get("id"))
            .and_then(Value::as_str)
            == active_turn_id
        && active_thread_id.is_some()
        && active_turn_id.is_some()
}

/// Keep the schema list honest: every top-level request field must be mapped
/// to a lossless descriptor field, a deny-on-presence field, or a documented
/// non-authority field. A future field cannot become Allow-capable merely by
/// being added to `schema_fields`.
fn approval_matrix_is_complete(entry: &ApprovalSchemaMatrixEntry) -> bool {
    entry.schema_fields.iter().all(|field| {
        entry
            .descriptor_fields
            .iter()
            .chain(entry.deny_allow_if_present)
            .chain(entry.non_authority_fields)
            .any(|classification| {
                *classification == *field
                    || classification
                        .strip_prefix(field)
                        .is_some_and(|suffix| suffix.starts_with('.') || suffix.starts_with('['))
            })
    })
}

impl ApprovalDescriptor {
    fn from_request(method: &str, params: &Value) -> (Self, bool) {
        let schema = approval_schema_variant(method);
        let (cwd, cwd_reviewable) = optional_approval_text(params, "cwd", MAX_APPROVAL_TEXT);
        let (reason, reason_reviewable) =
            optional_approval_text(params, "reason", MAX_APPROVAL_TEXT);
        let mut descriptor = Self {
            kind: approval_kind(method),
            reviewable: true,
            command: None,
            command_arguments: None,
            cwd,
            environment_id: None,
            network_approval: None,
            reason,
            file_changes: Vec::new(),
            requested_permissions: None,
            requested_permission_profile: None,
            permission_grant_scope: None,
            strict_auto_review: None,
        };
        // CWD and reason are part of the human decision context whenever the
        // generated request supplies them. A truncated, redacted, or
        // malformed value can still explain a denial, but not an informed
        // Allow.
        descriptor.reviewable &= cwd_reviewable && reason_reviewable;
        // The generated schema permits top-level extension fields. This
        // protocol owner does not: an extension has no reviewed authority
        // classification, so it is deny-only until added to the matrix.
        descriptor.reviewable &= schema.is_some_and(|entry| {
            entry.allow_supported
                && approval_matrix_is_complete(entry)
                && params_have_only_schema_fields(params, entry.schema_fields)
        });
        match method {
            "item/commandExecution/requestApproval" => {
                let (command, command_reviewable) =
                    optional_approval_text(params, "command", MAX_APPROVAL_COMMAND);
                descriptor.command = command;
                descriptor.reviewable &= command_reviewable && descriptor.command.is_some();
                let (environment_id, environment_reviewable) =
                    optional_approval_text(params, "environmentId", MAX_APPROVAL_TEXT);
                descriptor.environment_id = environment_id;
                descriptor.reviewable &= environment_reviewable;
                let (network_approval, network_reviewable) =
                    describe_network_approval(params.get("networkApprovalContext"));
                descriptor.network_approval = network_approval;
                descriptor.reviewable &= network_reviewable;
                // A missing/null working directory leaves relative command
                // execution bound to an undisclosed server default.
                descriptor.reviewable &= descriptor.cwd.is_some();
                // The owner only ever returns `accept`, never a persistent
                // cache/execpolicy/network amendment decision. Refuse Allow
                // rather than letting either schema proposal be invisible.
                descriptor.reviewable &= !has_non_null_field(params, "proposedExecpolicyAmendment")
                    && !has_non_null_field(params, "proposedNetworkPolicyAmendments");
            }
            "execCommandApproval" => {
                let Some(parts) = params.get("command").and_then(Value::as_array) else {
                    descriptor.reviewable = false;
                    return (descriptor, false);
                };
                if parts.len() > MAX_APPROVAL_LIST_ITEMS {
                    descriptor.reviewable = false;
                }
                let Some(parts) = parts.iter().map(Value::as_str).collect::<Option<Vec<_>>>()
                else {
                    descriptor.reviewable = false;
                    return (descriptor, false);
                };
                descriptor.reviewable &= parts
                    .iter()
                    .all(|part| approval_text_is_reviewable(part, MAX_APPROVAL_COMMAND));
                descriptor.command_arguments = Some(
                    parts
                        .iter()
                        .map(|part| sanitize_approval_text(part, MAX_APPROVAL_COMMAND))
                        .collect(),
                );
                descriptor.reviewable &= descriptor.cwd.is_some();
            }
            "item/fileChange/requestApproval" => {
                let (root, root_reviewable) =
                    optional_approval_text(params, "grantRoot", MAX_APPROVAL_TEXT);
                if let Some(root) = root {
                    descriptor.file_changes.push(ApprovalFileChange {
                        path: root,
                        operation: "grant_root".to_string(),
                        move_path: None,
                        content: None,
                        unified_diff: None,
                    });
                }
                // The accept response approves the correlated
                // FileChangeThreadItem.changes, but those paths, kinds, and
                // patch bytes are absent from this request. Never register it
                // as actionable until the protocol delivers and binds that
                // exact payload through a bounded review path.
                descriptor.reviewable &= root_reviewable
                    && !descriptor.file_changes.is_empty()
                    && schema.is_some_and(|entry| entry.allow_supported);
            }
            "applyPatchApproval" => {
                let (root, root_reviewable) =
                    optional_approval_text(params, "grantRoot", MAX_APPROVAL_TEXT);
                descriptor.reviewable &= root_reviewable;
                if let Some(root) = root {
                    descriptor.file_changes.push(ApprovalFileChange {
                        path: root,
                        operation: "grant_root".to_string(),
                        move_path: None,
                        content: None,
                        unified_diff: None,
                    });
                }
                let Some(changes) = params.get("fileChanges").and_then(Value::as_object) else {
                    descriptor.reviewable = false;
                    return (descriptor, false);
                };
                if changes.len() > MAX_APPROVAL_LIST_ITEMS {
                    descriptor.reviewable = false;
                }
                let mut patch_bytes = 0;
                for (path, change) in changes.iter().take(MAX_APPROVAL_LIST_ITEMS) {
                    let Some(change) = change.as_object() else {
                        descriptor.reviewable = false;
                        descriptor.file_changes.push(ApprovalFileChange {
                            path: sanitize_approval_text(path, MAX_APPROVAL_TEXT),
                            operation: "unknown".to_string(),
                            move_path: None,
                            content: None,
                            unified_diff: None,
                        });
                        continue;
                    };
                    let operation = change
                        .get("type")
                        .and_then(Value::as_str)
                        .filter(|kind| matches!(*kind, "add" | "update" | "delete"));
                    descriptor.reviewable &=
                        operation.is_some() && approval_text_is_reviewable(path, MAX_APPROVAL_TEXT);
                    let (content, unified_diff, move_path, change_reviewable) = match operation {
                        Some("add" | "delete") => {
                            let (content, content_reviewable) =
                                exact_patch_text(change, "content", &mut patch_bytes);
                            (
                                content,
                                None,
                                None,
                                content_reviewable && has_exact_keys(change, &["type", "content"]),
                            )
                        }
                        Some("update") => {
                            let (unified_diff, diff_reviewable) =
                                exact_patch_text(change, "unified_diff", &mut patch_bytes);
                            let (move_path, move_reviewable) =
                                optional_patch_path(change, "move_path");
                            (
                                None,
                                unified_diff,
                                move_path,
                                diff_reviewable
                                    && move_reviewable
                                    && has_exact_keys(
                                        change,
                                        &["type", "unified_diff", "move_path"],
                                    ),
                            )
                        }
                        _ => (None, None, None, false),
                    };
                    descriptor.reviewable &= change_reviewable;
                    descriptor.file_changes.push(ApprovalFileChange {
                        path: sanitize_approval_text(path, MAX_APPROVAL_TEXT),
                        operation: operation.unwrap_or("unknown").to_string(),
                        move_path,
                        content,
                        unified_diff,
                    });
                }
            }
            "item/permissions/requestApproval" => {
                let (environment_id, environment_reviewable) =
                    optional_approval_text(params, "environmentId", MAX_APPROVAL_TEXT);
                descriptor.environment_id = environment_id;
                descriptor.reviewable &= environment_reviewable && descriptor.cwd.is_some();
                if let Some(profile) = validated_requested_permissions(params) {
                    let (permissions, reviewable) = describe_permissions(&profile);
                    descriptor.requested_permissions = Some(permissions);
                    descriptor.reviewable &= reviewable;
                    if descriptor.reviewable {
                        // This is the very profile `approval_response` will
                        // return for Allow. Keeping the exact validated value
                        // alongside the summary prevents a future accepted
                        // field from becoming invisible to review.
                        descriptor.requested_permission_profile = Some(profile);
                        descriptor.permission_grant_scope = Some("turn");
                        descriptor.strict_auto_review = Some(true);
                    }
                    let allow_permitted = descriptor.reviewable;
                    return (descriptor, allow_permitted);
                }
                descriptor.reviewable = false;
                return (descriptor, false);
            }
            _ => {}
        }
        let allow_permitted = descriptor.reviewable;
        (descriptor, allow_permitted)
    }
}

/// An approval is not considered decided by the HTTP adapter until the owner
/// has flushed the original JSON-RPC response.  This acknowledgement crosses
/// only the local owner boundary; no response payload is retained.
#[derive(Debug)]
pub struct ApprovalCommand {
    pub decision: ApprovalDecision,
    pub delivered: oneshot::Sender<bool>,
    /// Cancellation holds the protocol reader at the response boundary until
    /// the owner has queued its real-ID interrupt. Ordinary decisions leave
    /// this empty and resume immediately after their wire acknowledgement.
    pub resume: Option<oneshot::Receiver<()>>,
}

/// Schema-shaped token-refresh hand-off.  Values are intentionally opaque to
/// every layer except the authenticated provider and JSON-RPC writer.
#[derive(Debug)]
pub struct PendingAuthRefresh {
    pub reason: String,
    pub previous_account_id: Option<String>,
    pub response: oneshot::Sender<AuthRefreshResponse>,
}

#[derive(Debug)]
pub struct AuthRefreshResponse {
    pub access_token: String,
    pub chatgpt_account_id: String,
    pub chatgpt_plan_type: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ThreadStarted {
    pub thread_id: String,
    pub model: String,
}

#[derive(Debug, Clone)]
pub struct TurnCompleted {
    pub status: String,
}

pub struct CodexClient {
    rpc: JsonlClient,
    process: ChildProcess,
    state: SessionState,
    active_thread_id: Option<String>,
    active_turn_id: Option<String>,
    approval_policy: ApprovalPolicy,
    auth_refresh_policy: AuthRefreshPolicy,
    /// Requests can arrive while any ordinary RPC is awaited.  Keep this
    /// shared with that dispatch path so an id can never receive two answers.
    seen_approvals: Arc<Mutex<SeenApprovals>>,
}

#[derive(Clone, Copy)]
struct ActiveRoute<'a> {
    thread_id: Option<&'a str>,
    turn_id: Option<&'a str>,
}

impl CodexClient {
    pub fn new(
        process: ChildProcess,
        stdin: tokio::process::ChildStdin,
        stdout: tokio::process::ChildStdout,
    ) -> Self {
        Self::with_approval_policy(process, stdin, stdout, ApprovalPolicy::Deny)
    }

    pub fn with_approval_policy(
        process: ChildProcess,
        stdin: tokio::process::ChildStdin,
        stdout: tokio::process::ChildStdout,
        approval_policy: ApprovalPolicy,
    ) -> Self {
        Self::with_approval_policy_and_timeout(
            process,
            stdin,
            stdout,
            approval_policy,
            crate::jsonl::DEFAULT_WAIT_TIMEOUT,
        )
    }

    pub fn with_approval_policy_and_timeout(
        process: ChildProcess,
        stdin: tokio::process::ChildStdin,
        stdout: tokio::process::ChildStdout,
        approval_policy: ApprovalPolicy,
        wait_timeout: Duration,
    ) -> Self {
        Self {
            rpc: JsonlClient::with_timeout(stdin, stdout, wait_timeout),
            process,
            state: SessionState::new(),
            active_thread_id: None,
            active_turn_id: None,
            approval_policy,
            auth_refresh_policy: AuthRefreshPolicy::Unavailable,
            seen_approvals: Arc::new(Mutex::new(SeenApprovals::default())),
        }
    }

    pub fn with_auth_refresh_policy(mut self, policy: AuthRefreshPolicy) -> Self {
        self.auth_refresh_policy = policy;
        self
    }

    /// Send an RPC call and poison the session if the app-server response
    /// desyncs (oversized/malformed frame or an unexpected response id),
    /// so every direct `CodexClient` caller observes the poison, not just
    /// the higher-level doctor/run orchestration.
    async fn rpc_call(&mut self, method: &str, params: Value) -> Result<Value, ClientError> {
        self.rpc_call_with_delivery(method, params, None).await
    }

    async fn rpc_call_with_delivery(
        &mut self,
        method: &str,
        params: Value,
        delivery: Option<&RequestDelivery>,
    ) -> Result<Value, ClientError> {
        // JsonlClient delegates server requests back to this owner even while
        // an ordinary response is awaited. The cloned policy is an owner
        // capability (the external channel leads to the authenticated API),
        // never transport-side approval authority.
        let rpc = &self.rpc;
        let approval_policy = self.approval_policy.clone();
        let auth_refresh_policy = self.auth_refresh_policy.clone();
        let seen_approvals = Arc::clone(&self.seen_approvals);
        let active_thread_id = self.active_thread_id.clone();
        let active_turn_id = self.active_turn_id.clone();
        match rpc
            .call_with_server_request_handler_and_delivery(
                method,
                params,
                delivery,
                move |message| {
                    let approval_policy = approval_policy.clone();
                    let auth_refresh_policy = auth_refresh_policy.clone();
                    let seen_approvals = Arc::clone(&seen_approvals);
                    let active_thread_id = active_thread_id.clone();
                    let active_turn_id = active_turn_id.clone();
                    async move {
                        Self::dispatch_server_request_during_call(
                            rpc,
                            &approval_policy,
                            &auth_refresh_policy,
                            &seen_approvals,
                            active_thread_id.as_deref(),
                            active_turn_id.as_deref(),
                            message,
                        )
                        .await
                        .map_err(|_| JsonlError::ServerRequestDuringCall)
                    }
                },
            )
            .await
        {
            Ok(value) => Ok(value),
            Err(error) => {
                if error.is_desync() {
                    self.state.poison();
                    // A server request can be interleaved with any ordinary
                    // admission RPC.  Once its response has been attempted,
                    // restarting that whole flow would replay an irreversible
                    // approval boundary, so classify the later desync here
                    // rather than only in wait_turn_completed.
                    if !self
                        .seen_approvals
                        .lock()
                        .expect("approval id mutex poisoned")
                        .is_empty()
                    {
                        return Err(ClientError::UnresolvedApprovalRestart);
                    }
                }
                Err(error.into())
            }
        }
    }

    pub async fn initialize(&mut self) -> Result<Value, ClientError> {
        let initialized = self
            .rpc_call(
                "initialize",
                json!({
                    "clientInfo": {
                        "name": "spark-runner",
                        "version": env!("CARGO_PKG_VERSION"),
                    }
                }),
            )
            .await?;
        self.rpc.notify("initialized", json!({})).await?;
        Ok(initialized)
    }

    pub async fn account_read(&mut self) -> Result<Value, ClientError> {
        self.rpc_call("account/read", json!({})).await
    }

    pub async fn model_list(&mut self) -> Result<Value, ClientError> {
        self.rpc_call("model/list", json!({ "includeHidden": true }))
            .await
    }

    pub async fn rate_limits_read(&mut self) -> Result<Value, ClientError> {
        self.rpc_call("account/rateLimits/read", json!({})).await
    }

    /// Admission checks shared by every live turn. They run before the first
    /// non-idempotent request, so a bad account/model/quota state consumes no
    /// turn or approval capacity.
    pub async fn admit_live_turn(&mut self) -> Result<Value, ClientError> {
        let account = self.account_read().await?;
        if account.pointer("/account/type").and_then(Value::as_str) != Some("chatgpt") {
            return Err(ClientError::ChatGptAuthRequired);
        }
        let mut models = self.model_list().await?;
        let mut has_required_model = false;
        for _ in 0..MAX_MODEL_LIST_PAGES {
            if model_list_contains_required_model(&models) {
                has_required_model = true;
                break;
            }
            let Some(cursor) = models.get("nextCursor") else {
                break;
            };
            if cursor.is_null() {
                break;
            }
            let Some(cursor) = cursor
                .as_str()
                .filter(|cursor| cursor.len() <= MAX_MODEL_CURSOR_LEN)
            else {
                self.state.poison();
                return Err(ClientError::SessionPoisoned);
            };
            models = self
                .rpc_call(
                    "model/list",
                    json!({ "cursor": cursor, "includeHidden": true }),
                )
                .await?;
        }
        if !has_required_model {
            return Err(fallback_model(
                "missing_from_model_list",
                "missing-from-model-list",
            ));
        }
        let rate_limits = self.rate_limits_read().await?;
        // A secondary window being available does not override an exhausted
        // primary bucket (nor a workspace-credit exhaustion).  The native
        // 0.144.3 response deliberately carries both the legacy single view
        // and the metered-by-limit view; every advertised bucket must be
        // usable before we spend a non-idempotent turn request.
        let has_quota = quota_available(&rate_limits);
        if !has_quota {
            return Err(ClientError::QuotaUnavailable);
        }
        Ok(rate_limits)
    }

    /// Start an ephemeral, read-only, on-request-approval thread pinned to
    /// `REQUIRED_MODEL`. Fails closed if the server reports a different model.
    pub async fn thread_start(&mut self, cwd: &Path) -> Result<ThreadStarted, ClientError> {
        self.thread_start_with_delivery(cwd, None).await
    }

    pub async fn thread_start_with_delivery(
        &mut self,
        cwd: &Path,
        delivery: Option<&RequestDelivery>,
    ) -> Result<ThreadStarted, ClientError> {
        let params = json!({
            "sandbox": "read-only",
            "approvalPolicy": "on-request",
            "ephemeral": true,
            "model": REQUIRED_MODEL,
            "cwd": cwd.to_string_lossy(),
        });
        let result = self
            .rpc_call_with_delivery("thread/start", params, delivery)
            .await?;

        let thread_id = result
            .get("thread")
            .and_then(|thread| thread.get("id"))
            .and_then(Value::as_str)
            .ok_or(ClientError::MissingThreadId)?
            .to_string();
        let model = result
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();

        if model != REQUIRED_MODEL {
            self.state.poison();
            return Err(fallback_model("thread_start_model", &model));
        }

        self.state.on_thread_started()?;
        self.active_thread_id = Some(thread_id.clone());
        Ok(ThreadStarted { thread_id, model })
    }

    pub async fn turn_start(
        &mut self,
        thread_id: &str,
        prompt: &str,
    ) -> Result<String, ClientError> {
        self.turn_start_with_delivery(thread_id, prompt, None).await
    }

    pub async fn turn_start_with_delivery(
        &mut self,
        thread_id: &str,
        prompt: &str,
        delivery: Option<&RequestDelivery>,
    ) -> Result<String, ClientError> {
        let params = json!({
            "threadId": thread_id,
            "input": [{ "type": "text", "text": prompt }],
        });
        let result = self
            .rpc_call_with_delivery("turn/start", params, delivery)
            .await?;
        self.state.on_turn_started()?;
        let turn_id = result
            .get("turn")
            .and_then(|turn| turn.get("id"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or(ClientError::MissingTurnId)?;
        self.active_turn_id = Some(turn_id.clone());
        Ok(turn_id)
    }

    /// Wait for the terminal `turn/completed` notification while the same
    /// owner task handles server-initiated approval requests. Raw model output
    /// is intentionally not extracted or logged here — only the status field.
    pub async fn wait_turn_completed(&mut self) -> Result<TurnCompleted, ClientError> {
        loop {
            let message = match self.rpc.next_message().await {
                Ok(message) => message,
                Err(error) => {
                    if error.is_desync() {
                        self.state.poison();
                        if !self
                            .seen_approvals
                            .lock()
                            .expect("approval id mutex poisoned")
                            .is_empty()
                        {
                            return Err(ClientError::UnresolvedApprovalRestart);
                        }
                    }
                    return Err(error.into());
                }
            };

            if let Some(method) = message.get("method").and_then(Value::as_str) {
                if message.get("id").is_some() {
                    self.handle_server_request(&message).await?;
                    continue;
                }
                if method == "turn/completed" {
                    let params = message.get("params").unwrap_or(&Value::Null);
                    if !terminal_route_matches(
                        params,
                        self.active_thread_id.as_deref(),
                        self.active_turn_id.as_deref(),
                    ) {
                        self.state.poison();
                        return Err(ClientError::SessionPoisoned);
                    }
                    return self.handle_turn_completed(params);
                }
                if method == "model/rerouted" {
                    self.state.poison();
                    let observed = message
                        .get("params")
                        .and_then(|params| {
                            params
                                .get("model")
                                .or_else(|| params.get("toModel"))
                                .or_else(|| params.get("newModel"))
                        })
                        .and_then(Value::as_str)
                        .unwrap_or("rerouted")
                        .to_string();
                    return Err(fallback_model("model_rerouted", &observed));
                }
                // Notification names originate at the child.  Preserve only a
                // bounded class in diagnostics, never child-controlled text.
                tracing::debug!(
                    class = "non_terminal_notification",
                    "ignoring app-server notification"
                );
                continue;
            }

            if message.get("id").is_some() {
                self.state.poison();
                return Err(ClientError::UnexpectedResponseWhileWaiting);
            }
        }
    }

    fn handle_turn_completed(&mut self, params: &Value) -> Result<TurnCompleted, ClientError> {
        let status = params
            .get("turn")
            .and_then(|turn| turn.get("status"))
            .and_then(Value::as_str)
            .or_else(|| params.get("status").and_then(Value::as_str))
            .ok_or(ClientError::MissingTurnStatus)?
            .to_string();

        if status == "completed" {
            self.state.on_turn_completed()?;
        } else {
            self.state.on_turn_failed()?;
        }
        Ok(TurnCompleted { status })
    }

    async fn handle_server_request(&mut self, message: &Value) -> Result<(), ClientError> {
        Self::handle_server_request_parts(
            &self.rpc,
            &mut self.state,
            &self.approval_policy,
            &self.auth_refresh_policy,
            &self.seen_approvals,
            ActiveRoute {
                thread_id: self.active_thread_id.as_deref(),
                turn_id: self.active_turn_id.as_deref(),
            },
            message,
        )
        .await
    }

    async fn handle_server_request_parts(
        rpc: &JsonlClient,
        state: &mut SessionState,
        approval_policy: &ApprovalPolicy,
        auth_refresh_policy: &AuthRefreshPolicy,
        seen_approvals: &Arc<Mutex<SeenApprovals>>,
        active_route: ActiveRoute<'_>,
        message: &Value,
    ) -> Result<(), ClientError> {
        let method = message
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let id = message
            .get("id")
            .filter(is_request_id)
            .cloned()
            .ok_or(ClientError::MissingServerRequestId)?;
        if method == "account/chatgptAuthTokens/refresh" {
            return Self::handle_auth_refresh(rpc, state, auth_refresh_policy, id, message).await;
        }
        if !is_known_approval_method(method) {
            rpc.respond_error(id, -32601, "method not found").await?;
            state.poison();
            return Err(ClientError::UnknownServerRequest);
        }

        let params = message.get("params").unwrap_or(&Value::Null);
        if !approval_route_matches(method, params, active_route.thread_id, active_route.turn_id) {
            rpc.respond(
                id,
                approval_response(method, ApprovalDecision::Deny, Some(params)),
            )
            .await?;
            state.poison();
            return Err(ClientError::SessionPoisoned);
        }
        let request_key = approval_request_key(method, &id, params);
        let seen = seen_approvals
            .lock()
            .expect("approval id mutex poisoned")
            .insert(request_key.clone())?;
        if !seen {
            rpc.respond(
                id.clone(),
                approval_response(method, ApprovalDecision::Deny, Some(params)),
            )
            .await?;
            state.poison();
            return Err(ClientError::DuplicateApproval { request_key });
        }

        state.on_approval_requested(request_key.clone(), method.to_string())?;
        let (descriptor, allow_permitted) = ApprovalDescriptor::from_request(method, params);
        let (decision, acknowledgement, resume) = match approval_policy {
            ApprovalPolicy::AllowForTests => (ApprovalDecision::Allow, None, None),
            ApprovalPolicy::Deny => (ApprovalDecision::Deny, None, None),
            ApprovalPolicy::External {
                pending,
                timeout,
                receipt,
            } => {
                let (decision_tx, decision_rx) = oneshot::channel();
                let pending_approval = PendingApproval {
                    request_key: request_key.clone(),
                    method: method.to_string(),
                    descriptor,
                    allow_permitted,
                    deadline: *timeout,
                    decision: decision_tx,
                };
                if let Some(receipt) = receipt {
                    receipt
                        .journal
                        .append(JournalEvent::ApprovalRequested {
                            execution_id: receipt.execution_id.clone(),
                            request_key: request_key.clone(),
                            method: method.to_string(),
                        })
                        .await
                        .map_err(|_| ClientError::SessionPoisoned)?;
                }
                if pending.send(pending_approval).await.is_err() {
                    (ApprovalDecision::Deny, None, None)
                } else {
                    // The owner schedules this same deadline.  Leave a
                    // narrow transport grace here solely to avoid retaining a
                    // request if the owner task itself has disappeared; the
                    // normal timeout always enters owner cancellation first.
                    match tokio::time::timeout(
                        timeout.saturating_add(Duration::from_secs(1)),
                        decision_rx,
                    )
                    .await
                    {
                        Ok(Ok(command)) => {
                            (command.decision, Some(command.delivered), command.resume)
                        }
                        // A closed local authority or deadline must still
                        // receive one schema-valid fail-closed response on the
                        // original JSON-RPC request id.
                        Ok(Err(_)) | Err(_) => (ApprovalDecision::Timeout, None, None),
                    }
                }
            }
        };
        let decision = if decision == ApprovalDecision::Allow && !allow_permitted {
            ApprovalDecision::Deny
        } else {
            decision
        };
        // Persist the decision before placing an Allow on the wire.  A
        // journal failure can therefore never leave an unrecorded grant in
        // the child, while a later wire failure remains a durable incident
        // that recovery can classify conservatively.
        append_external_approval_decision(approval_policy, &request_key, method, decision).await?;
        if let Err(error) = rpc
            .respond(id, approval_response(method, decision, Some(params)))
            .await
        {
            if let Some(acknowledgement) = acknowledgement {
                let _ = acknowledgement.send(false);
            }
            return Err(error.into());
        }
        state.on_approval_decided(
            request_key.clone(),
            method.to_string(),
            decision,
            ApprovalSource::Owner,
        )?;
        if let Some(acknowledgement) = acknowledgement {
            let _ = acknowledgement.send(true);
            if let Some(resume) = resume {
                let _ = resume.await;
            }
            tokio::task::yield_now().await;
        }
        Ok(())
    }

    async fn dispatch_server_request_during_call(
        rpc: &JsonlClient,
        approval_policy: &ApprovalPolicy,
        auth_refresh_policy: &AuthRefreshPolicy,
        seen_approvals: &Arc<Mutex<SeenApprovals>>,
        active_thread_id: Option<&str>,
        active_turn_id: Option<&str>,
        message: Value,
    ) -> Result<(), ClientError> {
        let id = message
            .get("id")
            .filter(is_request_id)
            .cloned()
            .ok_or(ClientError::MissingServerRequestId)?;
        let method = message
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if method == "account/chatgptAuthTokens/refresh" {
            let mut state = SessionState::new();
            return Self::handle_auth_refresh(rpc, &mut state, auth_refresh_policy, id, &message)
                .await;
        }
        if !is_known_approval_method(method) {
            rpc.respond_error(id, -32601, "method not found").await?;
            return Err(ClientError::UnknownServerRequest);
        }
        let params = message.get("params").unwrap_or(&Value::Null);
        if !approval_route_matches(method, params, active_thread_id, active_turn_id) {
            rpc.respond(
                id,
                approval_response(method, ApprovalDecision::Deny, Some(params)),
            )
            .await?;
            return Err(ClientError::SessionPoisoned);
        }
        let request_key = approval_request_key(method, &id, params);
        let seen = seen_approvals
            .lock()
            .expect("approval id mutex poisoned")
            .insert(request_key.clone())?;
        if !seen {
            rpc.respond(
                id,
                approval_response(method, ApprovalDecision::Deny, Some(params)),
            )
            .await?;
            return Err(ClientError::DuplicateApproval { request_key });
        }
        let (descriptor, allow_permitted) = ApprovalDescriptor::from_request(method, params);
        let (decision, acknowledgement, resume) = match approval_policy {
            ApprovalPolicy::AllowForTests => (ApprovalDecision::Allow, None, None),
            ApprovalPolicy::Deny => (ApprovalDecision::Deny, None, None),
            ApprovalPolicy::External {
                pending,
                timeout,
                receipt,
            } => {
                let (decision_tx, decision_rx) = oneshot::channel();
                if let Some(receipt) = receipt {
                    receipt
                        .journal
                        .append(JournalEvent::ApprovalRequested {
                            execution_id: receipt.execution_id.clone(),
                            request_key: request_key.clone(),
                            method: method.to_string(),
                        })
                        .await
                        .map_err(|_| ClientError::SessionPoisoned)?;
                }
                if pending
                    .send(PendingApproval {
                        request_key: request_key.clone(),
                        method: method.to_string(),
                        descriptor,
                        allow_permitted,
                        deadline: *timeout,
                        decision: decision_tx,
                    })
                    .await
                    .is_err()
                {
                    // This branch is reachable while an ordinary RPC is
                    // outstanding.  It is still a genuine server request,
                    // so fail closed *on the original id* rather than
                    // returning an internal hand-off error without a wire
                    // response.
                    (ApprovalDecision::Timeout, None, None)
                } else {
                    match tokio::time::timeout(
                        timeout.saturating_add(Duration::from_secs(1)),
                        decision_rx,
                    )
                    .await
                    {
                        Ok(Ok(command)) => {
                            (command.decision, Some(command.delivered), command.resume)
                        }
                        Ok(Err(_)) | Err(_) => (ApprovalDecision::Timeout, None, None),
                    }
                }
            }
        };
        let decision = if decision == ApprovalDecision::Allow && !allow_permitted {
            ApprovalDecision::Deny
        } else {
            decision
        };
        append_external_approval_decision(approval_policy, &request_key, method, decision).await?;
        if let Err(error) = rpc
            .respond(id, approval_response(method, decision, Some(params)))
            .await
        {
            if let Some(acknowledgement) = acknowledgement {
                let _ = acknowledgement.send(false);
            }
            return Err(error.into());
        }
        if let Some(acknowledgement) = acknowledgement {
            let _ = acknowledgement.send(true);
            if let Some(resume) = resume {
                let _ = resume.await;
            }
            tokio::task::yield_now().await;
        }
        Ok(())
    }

    async fn handle_auth_refresh(
        rpc: &JsonlClient,
        state: &mut SessionState,
        policy: &AuthRefreshPolicy,
        id: Value,
        message: &Value,
    ) -> Result<(), ClientError> {
        let params = message.get("params").unwrap_or(&Value::Null);
        let reason = params
            .get("reason")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if reason != "unauthorized" {
            rpc.respond_error(id, -32602, "invalid refresh request")
                .await?;
            state.poison();
            return Err(ClientError::AuthTokensRefreshUnavailable);
        }
        let response = match policy {
            AuthRefreshPolicy::Unavailable => None,
            AuthRefreshPolicy::External { pending, timeout } => {
                let (tx, rx) = oneshot::channel();
                let request = PendingAuthRefresh {
                    reason: reason.to_string(),
                    previous_account_id: params
                        .get("previousAccountId")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned),
                    response: tx,
                };
                if pending.send(request).await.is_err() {
                    None
                } else {
                    tokio::time::timeout(*timeout, rx)
                        .await
                        .ok()
                        .and_then(Result::ok)
                }
            }
        };
        if let Some(response) = response {
            rpc.respond(
                id,
                json!({
                    "accessToken": response.access_token,
                    "chatgptAccountId": response.chatgpt_account_id,
                    "chatgptPlanType": response.chatgpt_plan_type,
                }),
            )
            .await?;
            Ok(())
        } else {
            rpc.respond_error(id, -32000, "authentication refresh unavailable")
                .await?;
            state.poison();
            Err(ClientError::AuthTokensRefreshUnavailable)
        }
    }

    /// Bounded, drained stderr tail from the child app-server process — for
    /// local diagnostics only; never written into evidence files.
    pub async fn stderr_tail(&self) -> String {
        self.process.stderr_tail().await
    }

    pub fn is_poisoned(&self) -> bool {
        self.state.is_poisoned()
    }

    pub fn internal_events(&self) -> &[InternalEvent] {
        self.state.events()
    }

    pub async fn shutdown(mut self) -> Result<(), ClientError> {
        let _ = self.state.on_shutdown();
        self.process.shutdown().await;
        Ok(())
    }

    /// Interrupt the exact live turn before process cleanup.  The generated
    /// 0.144.3 shape requires both identifiers; callers must not fabricate a
    /// terminal state without making this protocol attempt.
    pub async fn turn_interrupt(
        &mut self,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<(), ClientError> {
        self.turn_interrupt_with_delivery(thread_id, turn_id, None)
            .await
    }

    /// Send the exact live interrupt while exposing its first write attempt
    /// to the runtime owner. Once that attempt starts, a timeout cannot be
    /// reported as an ordinary deadline because the app-server may have
    /// received the cancellation request.
    pub async fn turn_interrupt_with_delivery(
        &mut self,
        thread_id: &str,
        turn_id: &str,
        delivery: Option<&RequestDelivery>,
    ) -> Result<(), ClientError> {
        self.rpc_call_with_delivery(
            "turn/interrupt",
            json!({ "threadId": thread_id, "turnId": turn_id }),
            delivery,
        )
        .await?;
        Ok(())
    }
}

async fn append_external_approval_decision(
    policy: &ApprovalPolicy,
    request_key: &str,
    method: &str,
    decision: ApprovalDecision,
) -> Result<(), ClientError> {
    let ApprovalPolicy::External {
        receipt: Some(receipt),
        ..
    } = policy
    else {
        return Ok(());
    };
    let decision = match decision {
        ApprovalDecision::Allow => crate::journal::ApprovalTerminalDecision::Allowed,
        ApprovalDecision::Deny => crate::journal::ApprovalTerminalDecision::Denied,
        ApprovalDecision::Timeout => crate::journal::ApprovalTerminalDecision::TimedOut,
    };
    receipt
        .journal
        .append(JournalEvent::ApprovalDecided {
            execution_id: receipt.execution_id.clone(),
            request_key: request_key.to_string(),
            method: method.to_string(),
            decision,
        })
        .await
        .map_err(|_| ClientError::SessionPoisoned)
}

fn rate_limit_windows(rate_limits: &Value) -> Vec<&Value> {
    fn collect<'a>(windows: &mut Vec<&'a Value>, snapshot: &'a Value) {
        for key in ["primary", "secondary"] {
            if let Some(window) = snapshot.get(key).filter(|window| !window.is_null()) {
                windows.push(window);
            }
        }
    }
    let mut windows = Vec::new();
    if let Some(snapshot) = rate_limits.get("rateLimits") {
        collect(&mut windows, snapshot);
    }
    if let Some(by_id) = rate_limits
        .get("rateLimitsByLimitId")
        .and_then(Value::as_object)
    {
        for snapshot in by_id.values() {
            collect(&mut windows, snapshot);
        }
    }
    windows
}

fn credits_available(rate_limits: &Value) -> bool {
    fn snapshot_credits_available(snapshot: &Value) -> bool {
        snapshot.get("credits").is_none_or(|credits| {
            credits.is_null()
                || credits
                    .get("unlimited")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                || credits
                    .get("hasCredits")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
        })
    }
    snapshot_credits_available(rate_limits.get("rateLimits").unwrap_or(&Value::Null))
        && rate_limits
            .get("rateLimitsByLimitId")
            .and_then(Value::as_object)
            .is_none_or(|by_id| by_id.values().all(snapshot_credits_available))
}

fn quota_available(rate_limits: &Value) -> bool {
    let windows = rate_limit_windows(rate_limits);
    rate_limits
        .pointer("/rateLimits/rateLimitReachedType")
        .is_none_or(Value::is_null)
        && rate_limits
            .get("rateLimitsByLimitId")
            .and_then(Value::as_object)
            .is_none_or(|by_id| {
                by_id.values().all(|snapshot| {
                    snapshot
                        .get("rateLimitReachedType")
                        .is_none_or(Value::is_null)
                })
            })
        && !windows.is_empty()
        && windows.into_iter().all(|window| {
            window
                .get("usedPercent")
                .and_then(Value::as_i64)
                .is_some_and(|used| (0..100).contains(&used))
        })
        && credits_available(rate_limits)
}

fn is_known_approval_method(method: &str) -> bool {
    approval_schema_variant(method).is_some()
}

fn is_request_id(value: &&Value) -> bool {
    value.as_str().is_some() || value.as_i64().is_some()
}

// Keep the complete serialized descriptor well below the API's 16 KiB event
// ceiling, so a child cannot make the one approval event silently disappear.
const MAX_APPROVAL_TEXT: usize = 160;
const MAX_APPROVAL_COMMAND: usize = 512;
const MAX_APPROVAL_LIST_ITEMS: usize = 16;
// Patch strings are copied byte-for-byte into the descriptor. Bound their
// aggregate source bytes before copying; the owner additionally admits the
// final escaped SSE envelope before it becomes actionable.
const MAX_APPROVAL_PATCH_BYTES: usize = 8 * 1024;
const MAX_PERMISSION_PROFILE_BYTES: usize = 8 * 1024;
const MAX_PERMISSION_ENTRIES: usize = 16;

fn approval_kind(method: &str) -> &'static str {
    match method {
        "item/commandExecution/requestApproval" | "execCommandApproval" => "command",
        "item/fileChange/requestApproval" | "applyPatchApproval" => "file_change",
        "item/permissions/requestApproval" => "permissions",
        _ => "unknown",
    }
}

fn has_non_null_field(params: &Value, name: &str) -> bool {
    params.get(name).is_some_and(|value| !value.is_null())
}

/// Copy an optional schema string only when it can be shown exactly. This is
/// intentionally stricter than `Value::as_str().is_none_or(...)`: a present
/// value of the wrong type must never masquerade as an absent authority field.
fn optional_approval_text(params: &Value, name: &str, limit: usize) -> (Option<String>, bool) {
    match params.get(name) {
        None | Some(Value::Null) => (None, true),
        Some(Value::String(value)) => (
            Some(sanitize_approval_text(value, limit)),
            approval_text_is_reviewable(value, limit),
        ),
        Some(_) => (None, false),
    }
}

fn describe_network_approval(value: Option<&Value>) -> (Option<ApprovalNetworkApproval>, bool) {
    let Some(value) = value else {
        return (None, true);
    };
    if value.is_null() {
        return (None, true);
    }
    let Some(context) = value.as_object() else {
        return (None, false);
    };
    if !has_only_keys(context, &["host", "protocol"]) {
        return (None, false);
    }
    let Some(host) = context.get("host").and_then(Value::as_str) else {
        return (None, false);
    };
    let Some(protocol) = context.get("protocol").and_then(Value::as_str) else {
        return (None, false);
    };
    let protocol = match protocol {
        "http" => "http",
        "https" => "https",
        "socks5Tcp" => "socks5Tcp",
        "socks5Udp" => "socks5Udp",
        _ => return (None, false),
    };
    let reviewable = approval_text_is_reviewable(host, MAX_APPROVAL_TEXT);
    (
        Some(ApprovalNetworkApproval {
            host: sanitize_approval_text(host, MAX_APPROVAL_TEXT),
            protocol,
        }),
        reviewable,
    )
}

/// Every accepted patch variant has exactly the keys defined by the 0.144.3
/// `FileChange` union. Rejecting extensions prevents an action-bearing byte
/// from being silently dropped from an otherwise approvable descriptor.
fn has_exact_keys(change: &serde_json::Map<String, Value>, allowed: &[&str]) -> bool {
    has_only_keys(change, allowed) && change.contains_key("type")
}

fn has_only_keys(value: &serde_json::Map<String, Value>, allowed: &[&str]) -> bool {
    value.keys().all(|key| allowed.contains(&key.as_str()))
}

/// The pinned schemas intentionally leave most top-level objects open for
/// forward compatibility. That is unsafe at an approval boundary: an unknown
/// field may change the effect of a generated response without appearing in
/// the authenticated descriptor. Only the matrix's complete schema field set
/// is therefore eligible for Allow.
fn params_have_only_schema_fields(params: &Value, allowed: &[&str]) -> bool {
    params
        .as_object()
        .is_some_and(|params| has_only_keys(params, allowed))
}

/// Preserve patch bytes exactly. A digest is deliberately not used here: it
/// cannot let a human independently review the requested content. Returning
/// `false` avoids copying over-budget content and makes Allow impossible.
fn exact_patch_text(
    change: &serde_json::Map<String, Value>,
    name: &str,
    total_bytes: &mut usize,
) -> (Option<String>, bool) {
    let Some(value) = change.get(name).and_then(Value::as_str) else {
        return (None, false);
    };
    let Some(next_total) = total_bytes.checked_add(value.len()) else {
        return (None, false);
    };
    if next_total > MAX_APPROVAL_PATCH_BYTES {
        return (None, false);
    }
    *total_bytes = next_total;
    (Some(value.to_string()), true)
}

fn optional_patch_path(
    change: &serde_json::Map<String, Value>,
    name: &str,
) -> (Option<String>, bool) {
    match change.get(name) {
        None | Some(Value::Null) => (None, true),
        Some(Value::String(value)) => (
            Some(sanitize_approval_text(value, MAX_APPROVAL_TEXT)),
            approval_text_is_reviewable(value, MAX_APPROVAL_TEXT),
        ),
        Some(_) => (None, false),
    }
}

/// Render only a bounded, human-reviewable summary.  Commands and reasons
/// are untrusted child text, so likely credential-bearing words, common token
/// prefixes, and test canaries are redacted before any authenticated SSE
/// recipient can observe them.
fn sanitize_approval_text(value: &str, limit: usize) -> String {
    let mut output = Vec::new();
    let mut redact_next = false;
    for word in value.split_whitespace() {
        let lower = word.to_ascii_lowercase();
        let sensitive = redact_next || sensitive_approval_word(&lower);
        redact_next = lower == "bearer" || lower.ends_with("bearer:");
        output.push(if sensitive {
            "[REDACTED]".to_string()
        } else {
            word.chars()
                .filter(|character| !character.is_control())
                .collect()
        });
    }
    let mut bounded = output.join(" ");
    if bounded.chars().count() > limit {
        bounded = bounded.chars().take(limit.saturating_sub(1)).collect();
        bounded.push('…');
    }
    bounded
}

/// A redacted descriptor can explain why a request was denied, but cannot
/// support an informed Allow for hidden command or filesystem scope.
fn approval_text_is_reviewable(value: &str, limit: usize) -> bool {
    value.chars().count() <= limit && sanitize_approval_text(value, limit) == value
}

fn sensitive_approval_word(lower: &str) -> bool {
    lower.contains("token")
        || lower.contains("secret")
        || lower.contains("password")
        || lower.contains("authorization")
        || lower.contains("canary")
        || lower.starts_with("sk-")
        || lower.starts_with("ghp_")
        || lower.starts_with("github_pat_")
        || lower.starts_with("xoxb-")
}

fn validated_requested_permissions(params: &Value) -> Option<Value> {
    let profile = params.get("permissions")?;
    let encoded = serde_json::to_vec(profile).ok()?;
    if encoded.len() > MAX_PERMISSION_PROFILE_BYTES {
        return None;
    }
    let profile = profile.as_object()?;
    if profile
        .keys()
        .any(|key| key != "fileSystem" && key != "network")
    {
        return None;
    }
    if !profile
        .get("fileSystem")
        .is_none_or(valid_file_system_permissions)
        || !profile.get("network").is_none_or(valid_network_permissions)
    {
        return None;
    }
    if permission_entry_count(profile) > MAX_PERMISSION_ENTRIES {
        return None;
    }
    Some(Value::Object(profile.clone()))
}

fn permission_entry_count(profile: &serde_json::Map<String, Value>) -> usize {
    profile
        .get("fileSystem")
        .and_then(Value::as_object)
        .map(|file_system| {
            ["entries", "read", "write"]
                .into_iter()
                .filter_map(|key| file_system.get(key).and_then(Value::as_array))
                .map(Vec::len)
                .sum()
        })
        .unwrap_or(0)
}

fn valid_file_system_permissions(value: &Value) -> bool {
    if value.is_null() {
        return true;
    }
    let Some(file_system) = value.as_object() else {
        return false;
    };
    if file_system.keys().any(|key| {
        !matches!(
            key.as_str(),
            "entries" | "globScanMaxDepth" | "read" | "write"
        )
    }) {
        return false;
    }
    if !file_system
        .get("entries")
        .is_none_or(|entries| entries.is_null() || valid_permission_entries(entries))
    {
        return false;
    }
    if !file_system
        .get("globScanMaxDepth")
        .is_none_or(|depth| depth.is_null() || depth.as_u64().is_some_and(|depth| depth > 0))
    {
        return false;
    }
    ["read", "write"].into_iter().all(|kind| {
        file_system.get(kind).is_none_or(|paths| {
            paths.is_null()
                || paths.as_array().is_some_and(|paths| {
                    paths.len() <= MAX_PERMISSION_ENTRIES
                        && paths.iter().all(|path| {
                            path.as_str()
                                .is_some_and(|path| path.chars().count() <= MAX_APPROVAL_TEXT)
                        })
                })
        })
    })
}

fn valid_permission_entries(value: &Value) -> bool {
    value.as_array().is_some_and(|entries| {
        entries.len() <= MAX_PERMISSION_ENTRIES
            && entries.iter().all(|entry| {
                let Some(entry) = entry.as_object() else {
                    return false;
                };
                if entry.keys().any(|key| key != "access" && key != "path")
                    || !matches!(
                        entry.get("access").and_then(Value::as_str),
                        Some("read" | "write" | "deny")
                    )
                {
                    return false;
                }
                valid_permission_path(entry.get("path").unwrap_or(&Value::Null))
            })
    })
}

fn valid_permission_path(value: &Value) -> bool {
    let Some(path) = value.as_object() else {
        return false;
    };
    let Some(kind) = path.get("type").and_then(Value::as_str) else {
        return false;
    };
    match kind {
        "path" => {
            path.keys().all(|key| key == "type" || key == "path")
                && path
                    .get("path")
                    .and_then(Value::as_str)
                    .is_some_and(|path| path.chars().count() <= MAX_APPROVAL_TEXT)
        }
        "glob_pattern" => {
            path.keys().all(|key| key == "type" || key == "pattern")
                && path
                    .get("pattern")
                    .and_then(Value::as_str)
                    .is_some_and(|pattern| pattern.chars().count() <= MAX_APPROVAL_TEXT)
        }
        "special" => {
            path.keys().all(|key| key == "type" || key == "value")
                && path.get("value").is_some_and(valid_special_permission_path)
        }
        _ => false,
    }
}

fn valid_special_permission_path(value: &Value) -> bool {
    let Some(path) = value.as_object() else {
        return false;
    };
    let Some(kind) = path.get("kind").and_then(Value::as_str) else {
        return false;
    };
    match kind {
        "root" | "minimal" | "tmpdir" | "slash_tmp" => path.len() == 1,
        "project_roots" => {
            path.keys().all(|key| key == "kind" || key == "subpath")
                && path.get("subpath").is_none_or(|subpath| {
                    subpath.is_null()
                        || subpath
                            .as_str()
                            .is_some_and(|value| value.chars().count() <= MAX_APPROVAL_TEXT)
                })
        }
        "unknown" => {
            path.keys()
                .all(|key| matches!(key.as_str(), "kind" | "path" | "subpath"))
                && path
                    .get("path")
                    .and_then(Value::as_str)
                    .is_some_and(|value| value.chars().count() <= MAX_APPROVAL_TEXT)
                && path.get("subpath").is_none_or(|subpath| {
                    subpath.is_null()
                        || subpath
                            .as_str()
                            .is_some_and(|value| value.chars().count() <= MAX_APPROVAL_TEXT)
                })
        }
        _ => false,
    }
}

fn valid_network_permissions(value: &Value) -> bool {
    value.is_null()
        || value.as_object().is_some_and(|network| {
            network.keys().all(|key| key == "enabled")
                && network
                    .get("enabled")
                    .is_none_or(|enabled| enabled.is_null() || enabled.is_boolean())
        })
}

fn describe_permissions(profile: &Value) -> (RequestedPermissions, bool) {
    let mut file_system = Vec::new();
    let mut reviewable = true;
    let file_system_profile = profile.get("fileSystem").and_then(Value::as_object);
    if let Some(file_system_profile) = file_system_profile {
        if let Some(entries) = file_system_profile.get("entries").and_then(Value::as_array) {
            for entry in entries {
                if let (Some(access), Some((path, safe))) = (
                    entry.get("access").and_then(Value::as_str),
                    describe_permission_path(entry.get("path").unwrap_or(&Value::Null)),
                ) {
                    reviewable &= safe;
                    file_system.push(RequestedFileSystemPermission {
                        access: access.to_string(),
                        ..path
                    });
                } else {
                    reviewable = false;
                }
            }
        }
        for (access, key) in [("read", "read"), ("write", "write")] {
            if let Some(paths) = file_system_profile.get(key).and_then(Value::as_array) {
                for path in paths {
                    if let Some(path) = path.as_str() {
                        reviewable &= approval_text_is_reviewable(path, MAX_APPROVAL_TEXT);
                        file_system.push(RequestedFileSystemPermission {
                            access: access.to_string(),
                            kind: "path",
                            path: Some(sanitize_approval_text(path, MAX_APPROVAL_TEXT)),
                            pattern: None,
                            special_kind: None,
                            subpath: None,
                        });
                    } else {
                        reviewable = false;
                    }
                }
            }
        }
    }
    (
        RequestedPermissions {
            file_system,
            network_enabled: profile.pointer("/network/enabled").and_then(Value::as_bool),
            glob_scan_max_depth: profile
                .pointer("/fileSystem/globScanMaxDepth")
                .and_then(Value::as_u64),
        },
        reviewable,
    )
}

fn describe_permission_path(value: &Value) -> Option<(RequestedFileSystemPermission, bool)> {
    let path = value.as_object()?;
    match path.get("type").and_then(Value::as_str)? {
        "path" => path.get("path").and_then(Value::as_str).map(|path| {
            (
                RequestedFileSystemPermission {
                    access: String::new(),
                    kind: "path",
                    path: Some(sanitize_approval_text(path, MAX_APPROVAL_TEXT)),
                    pattern: None,
                    special_kind: None,
                    subpath: None,
                },
                approval_text_is_reviewable(path, MAX_APPROVAL_TEXT),
            )
        }),
        "glob_pattern" => path.get("pattern").and_then(Value::as_str).map(|pattern| {
            (
                RequestedFileSystemPermission {
                    access: String::new(),
                    kind: "glob_pattern",
                    path: None,
                    pattern: Some(sanitize_approval_text(pattern, MAX_APPROVAL_TEXT)),
                    special_kind: None,
                    subpath: None,
                },
                approval_text_is_reviewable(pattern, MAX_APPROVAL_TEXT),
            )
        }),
        "special" => describe_special_permission_path(path.get("value")?),
        _ => None,
    }
}

fn describe_special_permission_path(
    value: &Value,
) -> Option<(RequestedFileSystemPermission, bool)> {
    let value = value.as_object()?;
    let special_kind = match value.get("kind").and_then(Value::as_str)? {
        "root" => "root",
        "minimal" => "minimal",
        "tmpdir" => "tmpdir",
        "slash_tmp" => "slash_tmp",
        "project_roots" => "project_roots",
        "unknown" => "unknown",
        _ => return None,
    };
    let path = value
        .get("path")
        .and_then(Value::as_str)
        .map(|path| sanitize_approval_text(path, MAX_APPROVAL_TEXT));
    let subpath = value
        .get("subpath")
        .and_then(Value::as_str)
        .map(|subpath| sanitize_approval_text(subpath, MAX_APPROVAL_TEXT));
    let reviewable = value
        .get("path")
        .and_then(Value::as_str)
        .is_none_or(|path| approval_text_is_reviewable(path, MAX_APPROVAL_TEXT))
        && value
            .get("subpath")
            .and_then(Value::as_str)
            .is_none_or(|subpath| approval_text_is_reviewable(subpath, MAX_APPROVAL_TEXT));
    Some((
        RequestedFileSystemPermission {
            access: String::new(),
            kind: "special",
            path,
            pattern: None,
            special_kind: Some(special_kind),
            subpath,
        },
        reviewable,
    ))
}

fn approval_request_key(method: &str, id: &Value, params: &Value) -> String {
    let stable = params
        .get("approvalId")
        .and_then(Value::as_str)
        .or_else(|| params.get("itemId").and_then(Value::as_str))
        .or_else(|| params.get("callId").and_then(Value::as_str))
        .unwrap_or("");
    // Do not expose the original id, approvalId, itemId, or callId to SSE,
    // SQLite, error text, or duplicate tracking. The original JSON-RPC id is
    // retained only in this stack frame long enough to send its response.
    let material = if stable.is_empty() {
        serde_json::to_string(id).unwrap_or_default()
    } else {
        stable.to_string()
    };
    format!("approval:{method}:{:016x}", {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        material.hash(&mut hasher);
        hasher.finish()
    })
}

fn approval_response(method: &str, decision: ApprovalDecision, params: Option<&Value>) -> Value {
    match method {
        "item/commandExecution/requestApproval" | "item/fileChange/requestApproval" => {
            let value = match decision {
                ApprovalDecision::Allow => "accept",
                ApprovalDecision::Deny => "cancel",
                ApprovalDecision::Timeout => "cancel",
            };
            json!({ "decision": value })
        }
        "execCommandApproval" | "applyPatchApproval" => {
            let value = match decision {
                ApprovalDecision::Allow => "approved",
                ApprovalDecision::Deny => "abort",
                ApprovalDecision::Timeout => "timed_out",
            };
            json!({ "decision": value })
        }
        "item/permissions/requestApproval" => {
            // 0.144.3 requires a GrantedPermissionProfile rather than a
            // decision enum. An explicit owner Allow grants the exact
            // profile requested on this one original request; Deny/timeout
            // use an empty profile. The profile is never persisted; a bounded
            // exact copy is exposed only in the authenticated review event.
            let permissions = match decision {
                ApprovalDecision::Allow => params
                    .and_then(validated_requested_permissions)
                    .unwrap_or_else(empty_permission_profile),
                ApprovalDecision::Deny | ApprovalDecision::Timeout => empty_permission_profile(),
            };
            json!({
                "permissions": permissions,
                "scope": "turn",
                "strictAutoReview": true
            })
        }
        _ => json!({ "decision": "cancel" }),
    }
}

fn empty_permission_profile() -> Value {
    json!({
        "fileSystem": { "entries": [] },
        "network": { "enabled": false }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Command context is bounded and redacted before it crosses the
    /// authenticated API/SSE boundary. A secret-shaped command is deny-only;
    /// patch contents are handled separately because a patch Allow requires
    /// their exact bytes to reach the reviewer.
    #[test]
    fn approval_descriptor_is_schema_aware_bounded_and_redacted() {
        let canary = "APPROVAL_SECRET_CANARY_DO_NOT_EXPOSE";
        let (command, allowed) = ApprovalDescriptor::from_request(
            "item/commandExecution/requestApproval",
            &json!({
                "command": format!("deploy --token {canary}"),
                "cwd": "/tmp/repo",
                "reason": "needs network access",
            }),
        );
        assert!(
            !allowed,
            "a redacted command cannot be approved without its hidden scope"
        );
        assert_eq!(command.kind, "command");
        assert!(!command.reviewable);
        assert_eq!(command.cwd.as_deref(), Some("/tmp/repo"));
        assert_eq!(command.reason.as_deref(), Some("needs network access"));
        let encoded = serde_json::to_string(&command).expect("descriptor json");
        assert!(!encoded.contains(canary));
        assert!(encoded.len() <= 2 * MAX_APPROVAL_COMMAND);

        let (permissions, allowed) = ApprovalDescriptor::from_request(
            "item/permissions/requestApproval",
            &json!({
                "cwd": "/tmp/repo",
                "permissions": {
                    "fileSystem": { "entries": [{
                        "access": "write",
                        "path": { "type": "path", "path": "/tmp/repo/output" }
                    }] },
                    "network": { "enabled": true }
                }
            }),
        );
        assert!(allowed);
        assert_eq!(permissions.kind, "permissions");
        assert_eq!(
            permissions
                .requested_permissions
                .as_ref()
                .expect("permission descriptor")
                .file_system[0]
                .access,
            "write"
        );
        assert_eq!(
            permissions
                .requested_permission_profile
                .as_ref()
                .expect("exact permission profile"),
            &json!({
                "fileSystem": { "entries": [{
                    "access": "write",
                    "path": { "type": "path", "path": "/tmp/repo/output" }
                }] },
                "network": { "enabled": true }
            })
        );

        let invalid =
            json!({ "permissions": { "network": { "enabled": true, "host": "unbounded" } } });
        let (invalid_descriptor, allowed) =
            ApprovalDescriptor::from_request("item/permissions/requestApproval", &invalid);
        assert!(!allowed);
        assert!(invalid_descriptor.requested_permissions.is_none());
        let response = approval_response(
            "item/permissions/requestApproval",
            ApprovalDecision::Allow,
            Some(&invalid),
        );
        assert_eq!(response["permissions"], empty_permission_profile());
    }

    #[test]
    fn approval_descriptor_exposes_special_permission_scope_or_refuses_allow() {
        let (descriptor, allowed) = ApprovalDescriptor::from_request(
            "item/permissions/requestApproval",
            &json!({
                "cwd": "/repo",
                "permissions": {
                    "fileSystem": { "entries": [
                        {
                            "access": "write",
                            "path": {
                                "type": "special",
                                "value": { "kind": "project_roots", "subpath": "generated" }
                            }
                        },
                        {
                            "access": "read",
                            "path": {
                                "type": "special",
                                "value": {
                                    "kind": "unknown",
                                    "path": "/tmp/external-root",
                                    "subpath": "inputs"
                                }
                            }
                        }
                    ] },
                    "network": { "enabled": true }
                }
            }),
        );
        assert!(allowed);
        assert!(descriptor.reviewable);
        let permissions = descriptor
            .requested_permissions
            .expect("permission descriptor");
        assert_eq!(permissions.file_system[0].kind, "special");
        assert_eq!(
            permissions.file_system[0].special_kind,
            Some("project_roots")
        );
        assert_eq!(
            permissions.file_system[0].subpath.as_deref(),
            Some("generated")
        );
        assert_eq!(permissions.file_system[1].special_kind, Some("unknown"));
        assert_eq!(
            permissions.file_system[1].path.as_deref(),
            Some("/tmp/external-root")
        );

        let (truncated, allowed) = ApprovalDescriptor::from_request(
            "applyPatchApproval",
            &json!({
                "fileChanges": (0..=MAX_APPROVAL_LIST_ITEMS)
                    .map(|index| (format!("/tmp/file-{index}"), json!({ "type": "update" })))
                    .collect::<serde_json::Map<String, Value>>()
            }),
        );
        assert!(!allowed);
        assert!(!truncated.reviewable);
        assert_eq!(truncated.file_changes.len(), MAX_APPROVAL_LIST_ITEMS);

        let (moved, allowed) = ApprovalDescriptor::from_request(
            "applyPatchApproval",
            &json!({
                "callId": "call-1",
                "conversationId": "thread-1",
                "fileChanges": {
                    "/repo/source.rs": {
                        "type": "update",
                        "unified_diff": "@@ -1 +1 @@\n-old\n+new\n",
                        "move_path": "/repo/destination.rs"
                    }
                }
            }),
        );
        assert!(allowed);
        assert!(moved.reviewable);
        assert_eq!(moved.file_changes.len(), 1);
        assert_eq!(moved.file_changes[0].path, "/repo/source.rs");
        assert_eq!(moved.file_changes[0].operation, "update");
        assert_eq!(
            moved.file_changes[0].move_path.as_deref(),
            Some("/repo/destination.rs"),
            "an Allow must not hide the move destination"
        );
        assert_eq!(
            moved.file_changes[0].unified_diff.as_deref(),
            Some("@@ -1 +1 @@\n-old\n+new\n"),
            "an Allow must not hide the exact update bytes"
        );

        let (unreviewable_move, allowed) = ApprovalDescriptor::from_request(
            "applyPatchApproval",
            &json!({
                "callId": "call-1",
                "conversationId": "thread-1",
                "fileChanges": {
                    "/repo/source.rs": {
                        "type": "update",
                        "unified_diff": "@@ -1 +1 @@\n-old\n+new\n",
                        "move_path": "/repo/two  spaces.rs"
                    }
                }
            }),
        );
        assert!(!allowed);
        assert!(!unreviewable_move.reviewable);
    }

    #[test]
    fn approval_descriptor_refuses_normalized_text_and_exposes_permission_profile() {
        let (command, allowed) = ApprovalDescriptor::from_request(
            "item/commandExecution/requestApproval",
            &json!({ "command": "printf  two-spaces" }),
        );
        assert!(!allowed);
        assert!(!command.reviewable);
        assert_eq!(command.command.as_deref(), Some("printf two-spaces"));

        let profile = json!({
            "fileSystem": {
                "globScanMaxDepth": 4,
                "entries": [{
                    "access": "read",
                    "path": { "type": "path", "path": "/repo/input" }
                }]
            },
            "network": { "enabled": false }
        });
        let (descriptor, allowed) = ApprovalDescriptor::from_request(
            "item/permissions/requestApproval",
            &json!({ "cwd": "/repo", "permissions": profile }),
        );
        assert!(allowed);
        let permissions = descriptor
            .requested_permissions
            .as_ref()
            .expect("permission summary");
        assert_eq!(permissions.glob_scan_max_depth, Some(4));
        assert_eq!(
            descriptor.requested_permission_profile,
            Some(profile.clone()),
            "the descriptor must show exactly the profile Allow returns"
        );
        assert_eq!(
            approval_response(
                "item/permissions/requestApproval",
                ApprovalDecision::Allow,
                Some(&json!({ "cwd": "/repo", "permissions": profile }))
            )["permissions"],
            profile
        );

        let (argv, allowed) = ApprovalDescriptor::from_request(
            "execCommandApproval",
            &json!({ "command": ["printf", "two  spaces"] }),
        );
        assert!(!allowed);
        assert_eq!(
            argv.command_arguments,
            Some(vec!["printf".to_string(), "two spaces".to_string()])
        );

        let (large_argv, allowed) = ApprovalDescriptor::from_request(
            "execCommandApproval",
            &json!({
                "command": vec!["界".repeat(512); MAX_APPROVAL_LIST_ITEMS],
                "cwd": "/repo",
            }),
        );
        assert!(
            allowed && large_argv.reviewable,
            "each exact argv element is valid even when their combined event is too large"
        );
        assert_eq!(
            large_argv.command_arguments,
            Some(vec!["界".repeat(512); MAX_APPROVAL_LIST_ITEMS])
        );
    }

    /// Schema-derived regression matrix for all approval methods supported by
    /// this owner. Every row supplies the special fields that change the
    /// action/scope and asserts the exact descriptor value an Allow exposes.
    #[test]
    fn approval_schema_matrix_losslessly_covers_allowable_authority() {
        struct MatrixCase {
            name: &'static str,
            method: &'static str,
            schema_fields: &'static [&'static str],
            allow_supported: bool,
            unreviewable_authority: Option<&'static str>,
            params: Value,
            expected: Vec<(&'static str, Value)>,
        }

        let permission_profile = json!({
            "fileSystem": {
                "entries": [
                    {
                        "access": "write",
                        "path": { "type": "path", "path": "/repo/generated" }
                    },
                    {
                        "access": "read",
                        "path": { "type": "glob_pattern", "pattern": "/repo/**/*.rs" }
                    },
                    {
                        "access": "read",
                        "path": {
                            "type": "special",
                            "value": { "kind": "project_roots", "subpath": "inputs" }
                        }
                    },
                    {
                        "access": "deny",
                        "path": {
                            "type": "special",
                            "value": {
                                "kind": "unknown",
                                "path": "/outside",
                                "subpath": "private"
                            }
                        }
                    }
                ],
                "globScanMaxDepth": 4,
                "read": ["/legacy/read"],
                "write": ["/legacy/write"]
            },
            "network": { "enabled": true }
        });
        let cases = vec![
            MatrixCase {
                name: "command_execution_environment_and_network",
                method: "item/commandExecution/requestApproval",
                schema_fields: &[
                    "turnId",
                    "approvalId",
                    "threadId",
                    "command",
                    "commandActions",
                    "cwd",
                    "environmentId",
                    "itemId",
                    "networkApprovalContext",
                    "proposedExecpolicyAmendment",
                    "proposedNetworkPolicyAmendments",
                    "reason",
                    "startedAtMs",
                ],
                allow_supported: true,
                unreviewable_authority: None,
                params: json!({
                    "command": "curl https://registry.example/v1/index",
                    "cwd": "/repo",
                    "environmentId": "container-a",
                    "networkApprovalContext": {
                        "host": "registry.example",
                        "protocol": "https"
                    },
                    "reason": "fetch registry metadata",
                    "itemId": "item-1",
                    "threadId": "thread-1",
                    "turnId": "turn-1",
                    "startedAtMs": 1,
                    "commandActions": []
                }),
                expected: vec![
                    ("/command", json!("curl https://registry.example/v1/index")),
                    ("/cwd", json!("/repo")),
                    ("/environment_id", json!("container-a")),
                    ("/network_approval/host", json!("registry.example")),
                    ("/network_approval/protocol", json!("https")),
                ],
            },
            MatrixCase {
                name: "exec_command_exact_argv",
                method: "execCommandApproval",
                schema_fields: &[
                    "approvalId",
                    "callId",
                    "command",
                    "conversationId",
                    "cwd",
                    "parsedCmd",
                    "reason",
                ],
                allow_supported: true,
                unreviewable_authority: None,
                params: json!({
                    "approvalId": "approval-1",
                    "callId": "call-1",
                    "command": ["printf", "%s\\n", "two words"],
                    "conversationId": "thread-1",
                    "cwd": "/repo",
                    "parsedCmd": [{ "cmd": "printf", "type": "unknown" }],
                    "reason": "write a status line"
                }),
                expected: vec![
                    (
                        "/command_arguments",
                        json!(["printf", "%s\\n", "two words"]),
                    ),
                    ("/cwd", json!("/repo")),
                ],
            },
            MatrixCase {
                name: "apply_patch_add_delete_update_and_move",
                method: "applyPatchApproval",
                schema_fields: &[
                    "callId",
                    "conversationId",
                    "fileChanges",
                    "grantRoot",
                    "reason",
                ],
                allow_supported: true,
                unreviewable_authority: None,
                params: json!({
                    "callId": "call-1",
                    "conversationId": "thread-1",
                    "grantRoot": "/repo",
                    "reason": "apply reviewed changes",
                    "fileChanges": {
                        "/repo/add.rs": {
                            "type": "add",
                            "content": "pub fn added() {}\\n"
                        },
                        "/repo/delete.rs": {
                            "type": "delete",
                            "content": "obsolete  bytes\\n"
                        },
                        "/repo/update.rs": {
                            "type": "update",
                            "unified_diff": "@@ -1 +1 @@\\n-old\\n+new\\n",
                            "move_path": "/repo/moved.rs"
                        }
                    }
                }),
                expected: vec![
                    ("/file_changes/0/path", json!("/repo")),
                    ("/file_changes/1/content", json!("pub fn added() {}\\n")),
                    ("/file_changes/2/content", json!("obsolete  bytes\\n")),
                    (
                        "/file_changes/3/unified_diff",
                        json!("@@ -1 +1 @@\\n-old\\n+new\\n"),
                    ),
                    ("/file_changes/3/move_path", json!("/repo/moved.rs")),
                ],
            },
            MatrixCase {
                name: "file_change_unbound_correlated_changes",
                method: "item/fileChange/requestApproval",
                schema_fields: &[
                    "grantRoot",
                    "itemId",
                    "reason",
                    "startedAtMs",
                    "threadId",
                    "turnId",
                ],
                allow_supported: false,
                unreviewable_authority: Some("correlated FileChangeThreadItem.changes"),
                params: json!({
                    "grantRoot": "/repo/generated",
                    "reason": "allow writes under generated",
                    "itemId": "item-1",
                    "threadId": "thread-1",
                    "turnId": "turn-1",
                    "startedAtMs": 1
                }),
                expected: vec![
                    ("/file_changes/0/path", json!("/repo/generated")),
                    ("/file_changes/0/operation", json!("grant_root")),
                ],
            },
            MatrixCase {
                name: "permissions_full_profile_and_fixed_response_scope",
                method: "item/permissions/requestApproval",
                schema_fields: &[
                    "cwd",
                    "environmentId",
                    "itemId",
                    "permissions",
                    "reason",
                    "startedAtMs",
                    "threadId",
                    "turnId",
                ],
                allow_supported: true,
                unreviewable_authority: None,
                params: json!({
                    "cwd": "/repo",
                    "environmentId": "container-a",
                    "reason": "build generated output",
                    "permissions": permission_profile,
                    "itemId": "item-1",
                    "threadId": "thread-1",
                    "turnId": "turn-1",
                    "startedAtMs": 1
                }),
                expected: vec![
                    ("/cwd", json!("/repo")),
                    ("/environment_id", json!("container-a")),
                    ("/requested_permission_profile", permission_profile.clone()),
                    ("/requested_permissions/glob_scan_max_depth", json!(4)),
                    ("/permission_grant_scope", json!("turn")),
                    ("/strict_auto_review", json!(true)),
                ],
            },
        ];

        assert_eq!(APPROVAL_SCHEMA_MATRIX.len(), cases.len());
        let mut descriptors = Vec::new();
        for case in cases {
            let entry = APPROVAL_SCHEMA_MATRIX
                .iter()
                .find(|entry| entry.method == case.method)
                .unwrap_or_else(|| panic!("{} must be a declared schema variant", case.name));
            assert_eq!(
                entry.schema_fields, case.schema_fields,
                "{} must enumerate every top-level 0.144.3 schema field",
                case.name
            );
            assert_eq!(
                entry.allow_supported, case.allow_supported,
                "{} must declare whether an exact Allow review exists",
                case.name
            );
            assert_eq!(
                entry.unreviewable_authority, case.unreviewable_authority,
                "{} must document authority absent from the request",
                case.name
            );
            assert!(
                approval_matrix_is_complete(entry),
                "{} must classify every declared top-level field",
                case.name
            );
            let (descriptor, allowed) = ApprovalDescriptor::from_request(case.method, &case.params);
            assert_eq!(
                allowed && descriptor.reviewable,
                case.allow_supported,
                "{} Allow eligibility must match the schema matrix",
                case.name
            );
            let descriptor = serde_json::to_value(descriptor).expect("descriptor JSON");
            for (pointer, expected) in case.expected {
                assert_eq!(
                    descriptor.pointer(pointer),
                    Some(&expected),
                    "{} must expose {pointer} losslessly",
                    case.name
                );
            }
            descriptors.push((case.name, descriptor));
        }

        let patch_a = json!({
            "callId": "call-1",
            "conversationId": "thread-1",
            "fileChanges": {
                "/repo/update.rs": {
                    "type": "update",
                    "unified_diff": "@@ -1 +1 @@\\n-before\\n+after-a\\n"
                }
            }
        });
        let patch_b = json!({
            "callId": "call-1",
            "conversationId": "thread-1",
            "fileChanges": {
                "/repo/update.rs": {
                    "type": "update",
                    "unified_diff": "@@ -1 +1 @@\\n-before\\n+after-b\\n"
                }
            }
        });
        let (descriptor_a, allowed_a) =
            ApprovalDescriptor::from_request("applyPatchApproval", &patch_a);
        let (descriptor_b, allowed_b) =
            ApprovalDescriptor::from_request("applyPatchApproval", &patch_b);
        assert!(allowed_a && allowed_b);
        assert_ne!(
            serde_json::to_value(descriptor_a).expect("patch A descriptor"),
            serde_json::to_value(descriptor_b).expect("patch B descriptor"),
            "distinct patch payload bytes must not share an allowable descriptor"
        );
        assert!(
            descriptors
                .iter()
                .any(|(name, _)| *name == "apply_patch_add_delete_update_and_move"),
            "the table must cover the complete apply-patch union"
        );

        let deny_cases = [
            (
                "command execution without an exact cwd",
                "item/commandExecution/requestApproval",
                json!({
                    "command": "./relative-tool",
                    "itemId": "item-1",
                    "threadId": "thread-1",
                    "turnId": "turn-1",
                    "startedAtMs": 1
                }),
            ),
            (
                "command persistent proposal",
                "item/commandExecution/requestApproval",
                json!({
                    "command": "echo current-only",
                    "cwd": "/repo",
                    "proposedExecpolicyAmendment": ["echo *"]
                }),
            ),
            (
                "network policy proposal",
                "item/commandExecution/requestApproval",
                json!({
                    "command": "curl https://registry.example",
                    "cwd": "/repo",
                    "proposedNetworkPolicyAmendments": [
                        { "action": "allow", "host": "registry.example" }
                    ]
                }),
            ),
            (
                "command execution with unclassified top-level extension",
                "item/commandExecution/requestApproval",
                json!({
                    "command": "echo current-only",
                    "cwd": "/repo",
                    "futureAuthority": "hidden"
                }),
            ),
            (
                "exec command with unclassified top-level extension",
                "execCommandApproval",
                json!({
                    "command": ["echo", "current-only"],
                    "cwd": "/repo",
                    "futureAuthority": "hidden"
                }),
            ),
            (
                "add without required content",
                "applyPatchApproval",
                json!({
                    "fileChanges": { "/repo/add.rs": { "type": "add" } }
                }),
            ),
            (
                "update with unreviewed extension",
                "applyPatchApproval",
                json!({
                    "fileChanges": {
                        "/repo/update.rs": {
                            "type": "update",
                            "unified_diff": "@@ -1 +1 @@\\n-a\\n+b\\n",
                            "extraAuthority": "hidden"
                        }
                    }
                }),
            ),
            (
                "patch with unclassified top-level extension",
                "applyPatchApproval",
                json!({
                    "fileChanges": {
                        "/repo/add.rs": { "type": "add", "content": "exact" }
                    },
                    "futureAuthority": "hidden"
                }),
            ),
            (
                "patch content over bounded review budget",
                "applyPatchApproval",
                json!({
                    "fileChanges": {
                        "/repo/large.rs": {
                            "type": "add",
                            "content": "x".repeat(MAX_APPROVAL_PATCH_BYTES + 1)
                        }
                    }
                }),
            ),
            (
                "file change request lacks its correlated patch payload",
                "item/fileChange/requestApproval",
                json!({ "grantRoot": "/repo" }),
            ),
            (
                "permissions with unclassified top-level extension",
                "item/permissions/requestApproval",
                json!({
                    "cwd": "/repo",
                    "permissions": { "network": { "enabled": false } },
                    "futureAuthority": "hidden"
                }),
            ),
        ];
        for (name, method, params) in deny_cases {
            let (descriptor, allowed) = ApprovalDescriptor::from_request(method, &params);
            assert!(
                !allowed && !descriptor.reviewable,
                "{name} must make Allow impossible"
            );
        }
    }

    /// T10: canonical 0.144.3 quota admission is fail-closed.  An available
    /// secondary window never overrides an exhausted primary or an explicit
    /// reached type, and malformed snapshots do not become capacity.
    #[test]
    fn canonical_rate_limit_admission_rejects_partial_or_malformed_capacity() {
        let exhausted_primary = json!({
            "rateLimits": {
                "primary": { "usedPercent": 100 },
                "secondary": { "usedPercent": 0 },
                "rateLimitReachedType": null,
                "credits": null
            },
            "rateLimitsByLimitId": null
        });
        assert!(!quota_available(&exhausted_primary));
        assert!(!credits_available(
            &json!({ "rateLimits": { "credits": { "hasCredits": false, "unlimited": false } } })
        ));

        let reached = json!({
            "rateLimits": { "primary": { "usedPercent": 0 }, "rateLimitReachedType": "workspace_owner_credits_depleted", "credits": null },
            "rateLimitsByLimitId": null
        });
        assert!(!quota_available(&reached));
        assert!(!quota_available(&json!({ "rateLimits": {} })));
    }

    /// Remote model text is never retained in an error. The canary proves a
    /// reroute value cannot cross a Display/log boundary verbatim.
    #[test]
    fn reroute_diagnostic_retains_only_bounded_hash_metadata() {
        let canary = "MODEL_CANARY_please_do_not_render";
        let error = fallback_model("model_rerouted", canary);
        let rendered = error.to_string();
        assert!(!rendered.contains(canary));
        assert!(rendered.contains("class=model_rerouted"));
        assert!(rendered.contains("hash="));
    }

    /// T07/T10 diagnostic canary: child-controlled quota fields are reduced
    /// to allowlisted admission metadata before journal persistence.
    #[test]
    fn rate_limit_journal_snapshot_drops_untrusted_canary_fields() {
        let canary = "REMOTE_RATE_LIMIT_CANARY_do_not_persist";
        let snapshot = json!({
            "rateLimits": { "primary": { "usedPercent": 0 }, "credits": null },
            "rateLimitsByLimitId": null,
            "innocentLookingDiagnostic": canary,
        });
        let sanitized = journal_rate_limit_snapshot(&snapshot);
        let encoded = serde_json::to_string(&sanitized).unwrap();
        assert!(!encoded.contains(canary));
        assert_eq!(sanitized["class"], "rate_limit_snapshot");
        assert!(sanitized["hash"]
            .as_str()
            .is_some_and(|hash| hash.len() == 16));
        assert_eq!(sanitized["quota_available"], true);
    }

    /// CP6: opaque approval keys never retain child ids, and the duplicate
    /// detector has explicit count and byte limits even under repeated input.
    #[test]
    fn approval_identifier_tracking_is_opaque_and_bounded() {
        let canary = "CHILD_APPROVAL_ID_DO_NOT_EXPORT".repeat(4096);
        let key = approval_request_key(
            "item/commandExecution/requestApproval",
            &json!(-9001),
            &json!({ "approvalId": canary }),
        );
        assert!(key.len() < 128);
        assert!(!key.contains("CHILD_APPROVAL_ID_DO_NOT_EXPORT"));

        let mut seen = SeenApprovals::default();
        for index in 0..MAX_SEEN_APPROVALS {
            assert!(seen
                .insert(format!("approval:bounded:{index:016x}"))
                .unwrap());
        }
        assert!(matches!(
            seen.insert("approval:bounded:overflow".to_string()),
            Err(ClientError::SessionPoisoned)
        ));
    }

    #[test]
    fn protocol_messages_must_match_the_active_route() {
        let approval = json!({
            "threadId": "thread-1",
            "turnId": "turn-1",
            "approvalId": "approval-1",
            "command": "pwd"
        });
        assert!(approval_route_matches(
            "item/commandExecution/requestApproval",
            &approval,
            Some("thread-1"),
            Some("turn-1")
        ));
        assert!(!approval_route_matches(
            "item/commandExecution/requestApproval",
            &approval,
            Some("thread-1"),
            Some("turn-other")
        ));

        let terminal = json!({
            "threadId": "thread-1",
            "turn": { "id": "turn-1", "status": "completed" }
        });
        assert!(terminal_route_matches(
            &terminal,
            Some("thread-1"),
            Some("turn-1")
        ));
        assert!(!terminal_route_matches(
            &terminal,
            Some("thread-other"),
            Some("turn-1")
        ));
    }

    #[test]
    fn admission_failures_have_safe_operator_classes() {
        assert_eq!(
            ClientError::ChatGptAuthRequired.class(),
            "chatgpt_auth_required"
        );
        assert_eq!(ClientError::QuotaUnavailable.class(), "quota_unavailable");
        assert_eq!(
            ClientError::AuthTokensRefreshUnavailable.class(),
            "auth_refresh_unavailable"
        );
    }

    #[test]
    fn model_admission_accepts_the_schema_model_identifier() {
        let models = json!({
            "data": [{
                "id": "picker-entry",
                "model": REQUIRED_MODEL
            }]
        });
        assert!(model_list_contains_required_model(&models));
    }

    /// The generated 0.144.3 permissions response has no decision enum.
    /// Allow must therefore preserve the requested bounded-in-flight profile,
    /// while deny remains an empty fail-closed profile.
    #[test]
    fn permissions_allow_and_deny_have_distinct_schema_valid_profiles() {
        let params = json!({
            "permissions": {
                "fileSystem": { "entries": [{ "access": "read", "path": { "type": "special", "value": { "kind": "project_roots" } } }] },
                "network": { "enabled": true }
            }
        });
        let allowed = approval_response(
            "item/permissions/requestApproval",
            ApprovalDecision::Allow,
            Some(&params),
        );
        let denied = approval_response(
            "item/permissions/requestApproval",
            ApprovalDecision::Deny,
            Some(&params),
        );
        assert_eq!(allowed["permissions"], params["permissions"]);
        assert_eq!(denied["permissions"]["network"]["enabled"], false);
        assert_ne!(allowed, denied);
    }
}
