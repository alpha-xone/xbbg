use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array, Int64Array,
    LargeStringArray, StringArray, UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use rmcp::handler::server::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, Implementation, ServerCapabilities, ServerInfo};
use rmcp::transport::stdio;
use rmcp::{tool, tool_handler, tool_router, ErrorData, ServerHandler, ServiceExt};
use serde_json::{json, Map, Number, Value};
use tokio::sync::OnceCell;
use xbbg_async::engine::{Engine, EngineConfig, RequestParams, RetryPolicy, ServerAddr, Transport};
use xbbg_async::BlpAsyncError;
use xbbg_core::{AuthConfig, BlpError};

mod request_adapter;

use request_adapter::{
    bdh_request_params, bdib_request_params, bdp_request_params, bds_request_params,
    bflds_request_params, bql_request_params, bsrch_request_params, generic_request_params,
    BdhArgs, BdibArgs, BdpArgs, BdsArgs, BfldsArgs, BqlArgs, BsrchArgs, RequestArgs,
};

#[derive(Clone, Debug)]
struct ResultLimits {
    max_rows: usize,
    max_string_chars: usize,
}

struct XbbgMcpServer {
    #[allow(dead_code)] // read by rmcp's generated ServerHandler impl
    tool_router: ToolRouter<Self>,
    engine: OnceCell<Arc<Engine>>,
    engine_config: EngineConfig,
    result_limits: ResultLimits,
}

impl XbbgMcpServer {
    fn new_from_env() -> Result<Self, String> {
        let (engine_config, result_limits) = load_settings_from_env()?;
        Ok(Self {
            tool_router: Self::tool_router(),
            engine: OnceCell::new(),
            engine_config,
            result_limits,
        })
    }

    async fn engine(&self) -> Result<&Arc<Engine>, ErrorData> {
        self.engine
            .get_or_try_init(|| async {
                let config = self.engine_config.clone();
                tokio::task::spawn_blocking(move || {
                    Engine::start(config)
                        .map(Arc::new)
                        .map_err(map_request_error)
                })
                .await
                .map_err(|err| {
                    ErrorData::internal_error(format!("engine startup task failed: {err}"), None)
                })?
            })
            .await
    }

    async fn execute_request(&self, params: RequestParams) -> Result<CallToolResult, ErrorData> {
        let batch = self
            .engine()
            .await?
            .request(params)
            .await
            .map_err(map_request_error)?;
        let payload = record_batch_to_json(&batch, &self.result_limits)?;
        Ok(CallToolResult::structured(payload))
    }
}

#[tool_router]
impl XbbgMcpServer {
    #[tool(
        description = "Bloomberg reference data request (bdp). Returns structured JSON with schema metadata and bounded rows.",
        annotations(
            title = "Bloomberg Reference Data",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn bdp(
        &self,
        Parameters(args): Parameters<BdpArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.execute_request(bdp_request_params(args)?).await
    }

    #[tool(
        description = "Bloomberg historical data request (bdh). Dates must be YYYYMMDD or YYYY-MM-DD.",
        annotations(
            title = "Bloomberg Historical Data",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn bdh(
        &self,
        Parameters(args): Parameters<BdhArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.execute_request(bdh_request_params(args)?).await
    }

    #[tool(
        description = "Bloomberg bulk data request (bds). Uses the bulk extractor and requires exactly one bulk field.",
        annotations(
            title = "Bloomberg Bulk Data",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn bds(
        &self,
        Parameters(args): Parameters<BdsArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.execute_request(bds_request_params(args)?).await
    }

    #[tool(
        description = "Bloomberg intraday bar request (bdib). Datetimes must be ISO-8601 strings and interval must be positive.",
        annotations(
            title = "Bloomberg Intraday Bars",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn bdib(
        &self,
        Parameters(args): Parameters<BdibArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.execute_request(bdib_request_params(args)?).await
    }

    #[tool(
        description = "Bloomberg Query Language request (bql).",
        annotations(
            title = "Bloomberg Query Language",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn bql(
        &self,
        Parameters(args): Parameters<BqlArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.execute_request(bql_request_params(args)?).await
    }

    #[tool(
        description = "Bloomberg search request (bsrch). The domain selects the saved Bloomberg search, extra parameters are passed through as named request elements.",
        annotations(
            title = "Bloomberg Search",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn bsrch(
        &self,
        Parameters(args): Parameters<BsrchArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.execute_request(bsrch_request_params(args)?).await
    }

    #[tool(
        description = "Bloomberg field metadata lookup (bflds). Supply either concrete field ids or a search_spec, but not both.",
        annotations(
            title = "Bloomberg Field Metadata",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn bflds(
        &self,
        Parameters(args): Parameters<BfldsArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.execute_request(bflds_request_params(args)?).await
    }

    #[tool(
        description = "Generic Bloomberg request. Supports raw/custom service and operation strings, including RawRequest via request_operation.",
        annotations(
            title = "Bloomberg Raw Request",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn request(
        &self,
        Parameters(args): Parameters<RequestArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.execute_request(generic_request_params(args)?).await
    }
}

fn server_version() -> &'static str {
    option_env!("XBBG_MCP_VERSION").unwrap_or(env!("CARGO_PKG_VERSION"))
}

#[tool_handler]
impl ServerHandler for XbbgMcpServer {
    fn get_info(&self) -> ServerInfo {
        // The server only advertises tools for now; request execution stays lazy so stdio startup
        // does not require a live Bloomberg session before the client can initialize.
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(
                Implementation::new(env!("CARGO_PKG_NAME"), server_version())
                    .with_title("xbbg MCP")
                    .with_description(
                        "Request/response Bloomberg tools backed directly by xbbg-async. Current env configuration supports single-host connectivity, core auth modes, and request-pool tuning.",
                    ),
            )
            .with_instructions(
                "Use bdp, bdh, bds, bdib, bql, bsrch, bflds, or request. Results are JSON with schema metadata and bounded rows. This MCP server currently exposes host/port, selected auth env vars, and core pool settings rather than the full EngineConfig surface.",
            )
    }
}

fn load_settings_from_env() -> Result<(EngineConfig, ResultLimits), String> {
    // Keep the initial env surface intentionally narrow: expose the subset we can document and
    // support honestly for MCP deployment, rather than implying full parity with every EngineConfig knob.
    let mut config = EngineConfig::default();

    let host = env_string(&[
        "XBBG_MCP_HOST",
        "XBBG_MCP_SERVER_HOST",
        "XBBG_HOST",
        "XBBG_SERVER_HOST",
        "XBBG_SERVER",
    ])
    .unwrap_or_else(|| "localhost".to_string());
    let port = env_u16(&[
        "XBBG_MCP_PORT",
        "XBBG_MCP_SERVER_PORT",
        "XBBG_PORT",
        "XBBG_SERVER_PORT",
    ])?
    .unwrap_or(8194);
    config.transport = Transport::Direct(vec![ServerAddr::new(host, port)]);
    config.request_pool_size =
        env_usize(&["XBBG_MCP_REQUEST_POOL_SIZE", "XBBG_REQUEST_POOL_SIZE"])?
            .unwrap_or(config.request_pool_size);
    config.validation_mode = env_parsed(
        &["XBBG_MCP_VALIDATION_MODE", "XBBG_VALIDATION_MODE"],
        "validation_mode",
    )?
    .unwrap_or(config.validation_mode);
    config.field_cache_path = env_string(&["XBBG_MCP_FIELD_CACHE_PATH", "XBBG_FIELD_CACHE_PATH"])
        .map(PathBuf::from)
        .or_else(|| config.field_cache_path.clone());
    if let Some(warmup_services) = env_csv(&["XBBG_MCP_WARMUP_SERVICES", "XBBG_WARMUP_SERVICES"]) {
        config.warmup_services = warmup_services;
    }
    config.sdk_log_level = env_parsed(
        &["XBBG_MCP_SDK_LOG_LEVEL", "XBBG_SDK_LOG_LEVEL"],
        "sdk_log_level",
    )?
    .unwrap_or(config.sdk_log_level);
    config.num_start_attempts = env_usize(&[
        "XBBG_MCP_NUM_START_ATTEMPTS",
        "XBBG_NUM_START_ATTEMPTS",
        "XBBG_MAX_ATTEMPT",
    ])?
    .unwrap_or(config.num_start_attempts);
    config.auto_restart_on_disconnection = env_bool(&[
        "XBBG_MCP_AUTO_RESTART_ON_DISCONNECTION",
        "XBBG_AUTO_RESTART_ON_DISCONNECTION",
        "XBBG_AUTO_RESTART",
    ])?
    .unwrap_or(config.auto_restart_on_disconnection);
    config.retry_policy = RetryPolicy {
        max_retries: env_u32(&["XBBG_MCP_RETRY_MAX_RETRIES", "XBBG_RETRY_MAX_RETRIES"])?
            .unwrap_or(config.retry_policy.max_retries),
        initial_delay_ms: env_u64(&[
            "XBBG_MCP_RETRY_INITIAL_DELAY_MS",
            "XBBG_RETRY_INITIAL_DELAY_MS",
        ])?
        .unwrap_or(config.retry_policy.initial_delay_ms),
        backoff_factor: env_f64(&["XBBG_MCP_RETRY_BACKOFF_FACTOR", "XBBG_RETRY_BACKOFF_FACTOR"])?
            .unwrap_or(config.retry_policy.backoff_factor),
        max_delay_ms: env_u64(&["XBBG_MCP_RETRY_MAX_DELAY_MS", "XBBG_RETRY_MAX_DELAY_MS"])?
            .unwrap_or(config.retry_policy.max_delay_ms),
    };
    config.overflow_policy = env_parsed(
        &["XBBG_MCP_OVERFLOW_POLICY", "XBBG_OVERFLOW_POLICY"],
        "overflow_policy",
    )?
    .unwrap_or(config.overflow_policy);
    config.auth = build_auth_from_env()?;

    let result_limits = ResultLimits {
        max_rows: env_usize(&["XBBG_MCP_MAX_ROWS"])?.unwrap_or(500).max(1),
        max_string_chars: env_usize(&["XBBG_MCP_MAX_STRING_CHARS"])?
            .unwrap_or(2_048)
            .max(16),
    };

    Ok((config, result_limits))
}

fn build_auth_from_env() -> Result<Option<AuthConfig>, String> {
    let auth_method = env_string(&["XBBG_MCP_AUTH_METHOD", "XBBG_AUTH_METHOD"]);
    let app_name = env_string(&["XBBG_MCP_APP_NAME", "XBBG_APP_NAME"]);
    let dir_property = env_string(&["XBBG_MCP_DIR_PROPERTY", "XBBG_DIR_PROPERTY"]);
    let user_id = env_string(&["XBBG_MCP_USER_ID", "XBBG_USER_ID"]);
    let ip_address = env_string(&["XBBG_MCP_IP_ADDRESS", "XBBG_IP_ADDRESS"]);
    let token = env_string(&["XBBG_MCP_TOKEN", "XBBG_TOKEN"]);

    let Some(method) = auth_method.map(|value| value.to_ascii_lowercase()) else {
        if app_name.is_some()
            || dir_property.is_some()
            || user_id.is_some()
            || ip_address.is_some()
            || token.is_some()
        {
            return Err(
                "auth_method is required when auth-specific environment variables are set"
                    .to_string(),
            );
        }
        return Ok(None);
    };

    let auth = match method.as_str() {
        "" | "none" => None,
        "user" => Some(AuthConfig::User),
        "app" => Some(AuthConfig::App {
            app_name: required_env_value(&app_name, "app_name", &method)?,
        }),
        "userapp" => Some(AuthConfig::UserApp {
            app_name: required_env_value(&app_name, "app_name", &method)?,
        }),
        "dir" | "directory" => Some(AuthConfig::Directory {
            property_name: required_env_value(&dir_property, "dir_property", &method)?,
        }),
        "manual" => Some(AuthConfig::Manual {
            app_name: required_env_value(&app_name, "app_name", &method)?,
            user_id: required_env_value(&user_id, "user_id", &method)?,
            ip_address: required_env_value(&ip_address, "ip_address", &method)?,
        }),
        "token" => Some(AuthConfig::Token {
            token: required_env_value(&token, "token", &method)?,
        }),
        other => {
            return Err(format!(
                "invalid auth_method '{other}' (expected none, user, app, userapp, dir, manual, or token)"
            ));
        }
    };

    Ok(auth)
}

fn required_env_value(value: &Option<String>, field: &str, method: &str) -> Result<String, String> {
    value
        .clone()
        .ok_or_else(|| format!("{field} is required for auth_method={method}"))
}

fn env_string(keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        env::var(key)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    })
}

fn env_csv(keys: &[&str]) -> Option<Vec<String>> {
    keys.iter().find_map(|key| {
        env::var(key).ok().map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
    })
}

fn env_usize(keys: &[&str]) -> Result<Option<usize>, String> {
    env_parse(keys, "usize", str::parse)
}

fn env_u16(keys: &[&str]) -> Result<Option<u16>, String> {
    env_parse(keys, "u16", str::parse)
}

fn env_u32(keys: &[&str]) -> Result<Option<u32>, String> {
    env_parse(keys, "u32", str::parse)
}

fn env_u64(keys: &[&str]) -> Result<Option<u64>, String> {
    env_parse(keys, "u64", str::parse)
}

fn env_f64(keys: &[&str]) -> Result<Option<f64>, String> {
    env_parse(keys, "f64", str::parse)
}

fn env_bool(keys: &[&str]) -> Result<Option<bool>, String> {
    env_parse(keys, "bool", parse_bool)
}

fn env_parsed<T>(keys: &[&str], label: &str) -> Result<Option<T>, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    env_parse(keys, label, str::parse)
}

fn env_parse<T, E, F>(keys: &[&str], label: &str, parser: F) -> Result<Option<T>, String>
where
    F: Fn(&str) -> Result<T, E>,
    E: std::fmt::Display,
{
    for key in keys {
        if let Ok(raw) = env::var(key) {
            let value = raw.trim();
            if value.is_empty() {
                continue;
            }
            return parser(value)
                .map(Some)
                .map_err(|err| format!("invalid {label} in {key}: {err}"));
        }
    }
    Ok(None)
}

fn parse_bool(value: &str) -> Result<bool, String> {
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(format!("expected true/false style boolean, got '{value}'")),
    }
}

fn map_request_error(error: BlpAsyncError) -> ErrorData {
    match error {
        BlpAsyncError::ConfigError { detail } => ErrorData::invalid_params(detail, None),
        BlpAsyncError::Blp(blp_error) | BlpAsyncError::BlpError(blp_error) => {
            map_blp_error(blp_error)
        }
        other => ErrorData::internal_error(other.to_string(), None),
    }
}

fn map_blp_error(error: BlpError) -> ErrorData {
    match error {
        BlpError::InvalidArgument { detail } => ErrorData::invalid_params(detail, None),
        BlpError::SchemaOperationNotFound { service, operation } => ErrorData::invalid_params(
            format!("unknown Bloomberg operation '{operation}' for service '{service}'"),
            None,
        ),
        BlpError::SchemaElementNotFound { parent, name } => ErrorData::invalid_params(
            format!("unknown Bloomberg request element '{name}' under '{parent}'"),
            None,
        ),
        BlpError::SchemaTypeMismatch {
            element,
            expected,
            found,
        } => ErrorData::invalid_params(
            format!("type mismatch at '{element}': expected {expected}, found {found}"),
            None,
        ),
        BlpError::SchemaUnsupported { element, detail } => ErrorData::invalid_params(
            format!("unsupported schema construct at '{element}': {detail}"),
            None,
        ),
        BlpError::Validation { message, errors } => {
            let details = errors
                .iter()
                .map(|error| match &error.suggestion {
                    Some(suggestion) => {
                        format!(
                            "{}: {} (did you mean '{suggestion}'?)",
                            error.path, error.message
                        )
                    }
                    None => format!("{}: {}", error.path, error.message),
                })
                .collect::<Vec<_>>();
            let detail = if details.is_empty() {
                message
            } else {
                format!("{message}: {}", details.join("; "))
            };
            ErrorData::invalid_params(detail, None)
        }
        other => ErrorData::internal_error(other.to_string(), None),
    }
}

fn record_batch_to_json(batch: &RecordBatch, limits: &ResultLimits) -> Result<Value, ErrorData> {
    let schema = batch.schema();
    let schema_json = schema
        .fields()
        .iter()
        .map(|field| {
            json!({
                "name": field.name(),
                "data_type": field.data_type().to_string(),
                "nullable": field.is_nullable(),
            })
        })
        .collect::<Vec<_>>();

    let total_rows = batch.num_rows();
    let returned_rows = total_rows.min(limits.max_rows);
    let mut value_truncated = false;
    let mut rows = Vec::with_capacity(returned_rows);

    // MCP clients need structured JSON, not opaque Arrow IPC. We keep the Arrow schema in-band and
    // bound row/string sizes so a single large Bloomberg response cannot overwhelm a stdio client.
    for row_index in 0..returned_rows {
        let mut row = Map::with_capacity(schema.fields().len());
        for (field, column) in schema.fields().iter().zip(batch.columns()) {
            let value = array_cell_to_json(
                column,
                field.data_type(),
                row_index,
                limits.max_string_chars,
                &mut value_truncated,
            )?;
            row.insert(field.name().to_string(), value);
        }
        rows.push(Value::Object(row));
    }

    // Response diagnostics (xbbg.eid_data / xbbg.security_errors /
    // xbbg.field_exceptions) ride on the Arrow schema metadata as JSON;
    // re-parse them into structured JSON so MCP clients see real objects.
    let mut metadata = Map::new();
    for (key, value) in schema.metadata() {
        let parsed =
            serde_json::from_str::<Value>(value).unwrap_or_else(|_| Value::String(value.clone()));
        metadata.insert(key.clone(), parsed);
    }

    let mut result = json!({
        "schema": schema_json,
        "row_count": total_rows,
        "returned_rows": returned_rows,
        "truncated": {
            "rows": total_rows > returned_rows,
            "values": value_truncated,
        },
        "rows": rows,
    });
    if !metadata.is_empty() {
        result["metadata"] = Value::Object(metadata);
    }
    Ok(result)
}

fn array_cell_to_json(
    column: &ArrayRef,
    data_type: &DataType,
    row_index: usize,
    max_string_chars: usize,
    value_truncated: &mut bool,
) -> Result<Value, ErrorData> {
    if column.is_null(row_index) {
        return Ok(Value::Null);
    }

    let value = match data_type {
        DataType::Utf8 => {
            json_string_from_array(column, row_index, max_string_chars, value_truncated)?
        }
        DataType::LargeUtf8 => {
            json_large_string_from_array(column, row_index, max_string_chars, value_truncated)?
        }
        DataType::Boolean => {
            Value::Bool(downcast::<BooleanArray>(column, "Boolean")?.value(row_index))
        }
        DataType::Int32 => Value::Number(Number::from(
            downcast::<Int32Array>(column, "Int32")?.value(row_index),
        )),
        DataType::Int64 => int64_to_json(downcast::<Int64Array>(column, "Int64")?.value(row_index)),
        DataType::UInt32 => Value::Number(Number::from(
            downcast::<UInt32Array>(column, "UInt32")?.value(row_index),
        )),
        DataType::UInt64 => {
            uint64_to_json(downcast::<UInt64Array>(column, "UInt64")?.value(row_index))
        }
        DataType::Float32 => float_to_json(
            downcast::<Float32Array>(column, "Float32")?.value(row_index) as f64,
            column.as_ref(),
            row_index,
        )?,
        DataType::Float64 => float_to_json(
            downcast::<Float64Array>(column, "Float64")?.value(row_index),
            column.as_ref(),
            row_index,
        )?,
        DataType::Date32 => {
            let array = downcast::<Date32Array>(column, "Date32")?;
            truncate_json_string(
                array_value_to_string(array, row_index).map_err(display_error)?,
                max_string_chars,
                value_truncated,
            )
        }
        _ => truncate_json_string(
            array_value_to_string(column.as_ref(), row_index).map_err(display_error)?,
            max_string_chars,
            value_truncated,
        ),
    };

    Ok(value)
}

fn int64_to_json(value: i64) -> Value {
    const JS_SAFE_INTEGER_MIN: i64 = -9_007_199_254_740_991;
    const JS_SAFE_INTEGER_MAX: i64 = 9_007_199_254_740_991;

    if (JS_SAFE_INTEGER_MIN..=JS_SAFE_INTEGER_MAX).contains(&value) {
        Value::Number(Number::from(value))
    } else {
        Value::String(value.to_string())
    }
}

fn uint64_to_json(value: u64) -> Value {
    const JS_SAFE_INTEGER_MAX: u64 = 9_007_199_254_740_991;

    if value <= JS_SAFE_INTEGER_MAX {
        Value::Number(Number::from(value))
    } else {
        Value::String(value.to_string())
    }
}

fn float_to_json(value: f64, column: &dyn Array, row_index: usize) -> Result<Value, ErrorData> {
    match Number::from_f64(value) {
        Some(number) => Ok(Value::Number(number)),
        None => Ok(Value::String(
            array_value_to_string(column, row_index).map_err(display_error)?,
        )),
    }
}

fn json_string_from_array(
    column: &ArrayRef,
    row_index: usize,
    max_string_chars: usize,
    value_truncated: &mut bool,
) -> Result<Value, ErrorData> {
    let array = downcast::<StringArray>(column, "Utf8")?;
    Ok(truncate_json_string(
        array.value(row_index).to_string(),
        max_string_chars,
        value_truncated,
    ))
}

fn json_large_string_from_array(
    column: &ArrayRef,
    row_index: usize,
    max_string_chars: usize,
    value_truncated: &mut bool,
) -> Result<Value, ErrorData> {
    let array = downcast::<LargeStringArray>(column, "LargeUtf8")?;
    Ok(truncate_json_string(
        array.value(row_index).to_string(),
        max_string_chars,
        value_truncated,
    ))
}

fn truncate_json_string(value: String, max_chars: usize, value_truncated: &mut bool) -> Value {
    if value.chars().count() <= max_chars {
        return Value::String(value);
    }

    *value_truncated = true;
    let truncated = value.chars().take(max_chars).collect::<String>();
    Value::String(format!("{truncated}…"))
}

fn downcast<'a, T: 'static>(column: &'a ArrayRef, label: &str) -> Result<&'a T, ErrorData> {
    column.as_any().downcast_ref::<T>().ok_or_else(|| {
        ErrorData::internal_error(format!("failed to downcast Arrow column as {label}"), None)
    })
}

fn display_error(error: arrow::error::ArrowError) -> ErrorData {
    ErrorData::internal_error(format!("failed to format Arrow value: {error}"), None)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = XbbgMcpServer::new_from_env()?;
    server.serve(stdio()).await?.waiting().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn mcp_tool_names_and_input_schemas_are_stable() {
        let tools = XbbgMcpServer::tool_router().list_all();
        let names = tools
            .iter()
            .map(|tool| tool.name.as_ref())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            ["bdh", "bdib", "bdp", "bds", "bflds", "bql", "bsrch", "request"]
        );

        let tool_by_name = tools
            .iter()
            .map(|tool| (tool.name.as_ref(), tool))
            .collect::<HashMap<_, _>>();
        let bdp_schema = tool_by_name
            .get("bdp")
            .expect("bdp tool")
            .input_schema
            .get("properties")
            .expect("bdp properties");
        assert!(bdp_schema.get("tickers").is_some());
        assert!(bdp_schema.get("fields").is_some());
        assert!(bdp_schema.get("validate_fields").is_some());

        let generic_schema = tool_by_name
            .get("request")
            .expect("generic request tool")
            .input_schema
            .get("properties")
            .expect("request properties");
        assert!(generic_schema.get("request_operation").is_some());
        assert!(generic_schema.get("request_id").is_some());
        assert!(generic_schema.get("jsonElements").is_none());
    }
}
