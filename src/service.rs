use futures::stream::{FuturesUnordered, StreamExt};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::{Client, Error as HyperClientError};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tracing::{debug, warn};

use http_body_util::{BodyExt, Full};
use hyper::body::{Body, Bytes, Incoming};
use hyper::{Method, StatusCode};
use hyper::{Request, Response};
use hyper_rustls::HttpsConnector;

use crate::dsn;
use crate::request;
use crate::state::AppState;
use crate::config::DataCategory;

type GenericError = Box<dyn std::error::Error + Send + Sync>;
type HandlerResult<T> = std::result::Result<T, GenericError>;
type BoxBody = http_body_util::combinators::BoxBody<Bytes, hyper::Error>;
type HttpsClient = Client<HttpsConnector<HttpConnector>, Full<Bytes>>;

/// Max time to wait for a single outbound upstream response.
const OUTBOUND_TIMEOUT: Duration = Duration::from_secs(30);

type OutboundResult = (
    Result<Result<Response<Incoming>, HyperClientError>, tokio::time::error::Elapsed>,
    String,
    Instant,
);

pub async fn handle_request<B: Body>(
    req: Request<B>,
    state: Arc<AppState>,
) -> HandlerResult<Response<BoxBody>>
where
    B::Error: std::error::Error + Sync + Send + 'static,
{
    let method = req.method();
    let path = req.uri().path().to_string();

    metrics::counter!("handle_request.request", "path" => path.clone()).increment(1);
    let request_timer = Instant::now();
    if method == Method::GET && path == "/health" {
        handle_health(req)
    } else {
        let res = handle_proxy(req, state).await;
        metrics::histogram!("handle_proxy.duration").record(request_timer.elapsed());

        res
    }
}

pub fn handle_health(_req: Request<impl Body>) -> HandlerResult<Response<BoxBody>> {
    Ok(Response::builder()
        .status(StatusCode::OK)
        .body(full("ok"))
        .unwrap())
}

pub async fn handle_proxy<B: Body>(
    req: Request<B>,
    state: Arc<AppState>,
) -> HandlerResult<Response<BoxBody>>
where
    B::Error: std::error::Error + Sync + Send + 'static,
{
    let method = req.method().clone();
    let uri = req.uri().clone();
    let path = uri.path();
    let headers = req.headers().clone();

    if state.config.verbose {
        let formatted_headers = headers
            .iter()
            .map(|(key, value)| {
                let value = value.to_str().unwrap_or("<invalid>");
                format!(" {key}: {value}\n")
            })
            .reduce(|mut acc, item| {
                acc.push_str(item.as_ref());
                acc
            });
        debug!("Request: {method} {path}");
        debug!(
            "Headers:\n{}",
            formatted_headers.unwrap_or("Invalid headers".into())
        );
    }

    // All store/envelope requests are POST
    if method != Method::POST {
        metrics::counter!("handle_proxy.incorrect_method", "method" => method.to_string())
            .increment(1);
        debug!("Received a non POST request. method={}", method);

        let res = Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(full("Method not allowed"))
            .unwrap();
        return Ok(res);
    }
    // Find DSN public key in request
    let found_dsn = dsn::from_request(&uri, &headers);
    if found_dsn.is_none() {
        debug!("Could not find a DSN in the request headers or URI");
        metrics::counter!("handle_proxy.no_dsn").increment(1);

        // Still read & log the body in verbose mode for easier debugging.
        if state.config.verbose {
            let _ = request::read_and_decode_body(&state.config, req, &headers, None).await;
        }

        return Ok(bad_request_response());
    }
    // Match the public key with registered keys
    let public_key = found_dsn.unwrap();
    let keyring = match state.keymap.get(&public_key) {
        Some(v) => v,
        // If a DSN cannot be found -> empty response
        None => {
            debug!(
                "Could not find a matching DSN in the configured keys. Got DSN: {0}",
                public_key
            );
            metrics::counter!(
                "handle_proxy.unknown_dsn",
                "inbound_key" => public_key.clone(),
            )
            .increment(1);

            return Ok(bad_request_response());
        }
    };

    let body_bytes = match request::read_and_decode_body(
        &state.config,
        req,
        &headers,
        Some(&public_key),
    )
    .await
    {
        Ok(body) => body,
        Err(e) => {
            warn!("Could not read/ decode body for {method} {path}: {e}");
            return Ok(bad_request_response());
        }
    };

    // Detect data category (best-effort). Fail-open if unknown.
    let detected_category: Option<DataCategory> = request::detect_data_category(&uri, &body_bytes);

    // Fan out to all matching outbound DSNs concurrently. Use the first successful
    // response body for the client, and keep remaining outbounds running in the
    // background so slower upstreams (e.g. SaaS) are not cancelled.
    let mut responses = Vec::new();
    for outbound_target in keyring.outbound.iter() {
        // Apply per-outbound category filter if configured
        if let Some(ref categories) = outbound_target.categories {
            if let Some(ref cat) = detected_category {
                if !categories.contains(cat) {
                    // Skip this outbound if the category is not allowed
                    continue;
                }
            }
        }

        let outbound_dsn = &outbound_target.dsn;
        let outbound_host = outbound_dsn.host.clone();
        metrics::counter!(
            "handle_proxy.outbound_request.start",
            "outbound_host" => outbound_host.clone()
        )
        .increment(1);
        debug!("Creating outbound request for {0}", &outbound_host);

        let build_request_timer = Instant::now();
        let request_builder =
            request::make_outbound_request(&state.config, &uri, &headers, outbound_dsn);

        let body_out = if state.config.modify_envelope_header {
            match request::replace_envelope_dsn(&body_bytes, outbound_dsn) {
                Some(new_body) => new_body,
                None => body_bytes.clone(),
            }
        } else {
            body_bytes.clone()
        };

        let request = request_builder.body(Full::new(body_out));
        metrics::histogram!(
            "handle_proxy.build_request.duration",
            "outbound_host" => outbound_host.clone()
        )
        .record(build_request_timer.elapsed());

        if let Ok(outbound_request) = request {
            let fut_res =
                send_request(state.client.clone(), outbound_request, outbound_host.clone());
            responses.push(fut_res);
        } else {
            warn!("Could not build request {0:?}", request.err());
        }
    }
    if responses.is_empty() {
        warn!("No outbound requests made for {method} {path} (category: {:?})", detected_category);
    }

    let mut resp_body = Bytes::new();
    let mut unordered: FuturesUnordered<_> = responses.into_iter().collect();

    while let Some(outbound) = unordered.next().await {
        match settle_outbound(outbound).await {
            OutboundOutcome::Success { body } => {
                resp_body = body;
                // Do not cancel slower outbounds — finish them after returning to the client.
                if !unordered.is_empty() {
                    tokio::spawn(async move {
                        finish_remaining_outbounds(unordered).await;
                    });
                }
                break;
            }
            OutboundOutcome::Failed => {}
        }
    }

    // Add cors headers necessary for browser events
    let response_builder = Response::builder()
        .header("Access-Control-Allow-Origin", "*")
        .header(
            "Access-Control-Expose-Headers",
            "x-sentry-error,x-sentry-rate-limit,retry-after",
        )
        .header("Cross-Origin-Resource-Policy", "cross-origin");

    metrics::counter!(
        "handle_proxy.response",
        "inbound_key" => public_key.clone(),
    )
    .increment(1);

    Ok(response_builder.body(full(resp_body)).unwrap())
}

fn bad_request_response() -> Response<BoxBody> {
    Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .body(full("No DSN found"))
        .unwrap()
}

fn full<T: Into<Bytes>>(chunk: T) -> BoxBody {
    Full::new(chunk.into())
        .map_err(|never| match never {})
        .boxed()
}

enum OutboundOutcome {
    Success { body: Bytes },
    Failed,
}

/// Record metrics/logs for a completed outbound and drain or return its body.
async fn settle_outbound(outbound: OutboundResult) -> OutboundOutcome {
    let (response_res, resp_hostname, request_start) = outbound;
    match response_res {
        Ok(Ok(response)) => {
            debug!("Received response from {}", &resp_hostname);
            metrics::counter!(
                "handle_proxy.outbound_request.success",
                "outbound_host" => resp_hostname.clone(),
            )
            .increment(1);
            metrics::histogram!(
                "handle_proxy.send_request.duration",
                "outbound_host" => resp_hostname.clone()
            )
            .record(request_start.elapsed());

            match response.collect().await {
                Ok(response_body) => OutboundOutcome::Success {
                    body: response_body.to_bytes(),
                },
                Err(e) => {
                    metrics::counter!("handle_proxy.outbound_request.failed").increment(1);
                    warn!("Could not read response body from {resp_hostname}: {e:?}");
                    OutboundOutcome::Failed
                }
            }
        }
        Ok(Err(e)) => {
            metrics::counter!("handle_proxy.outbound_request.failed").increment(1);
            warn!("Could not make request to {resp_hostname}: {e:?}");
            OutboundOutcome::Failed
        }
        Err(_) => {
            metrics::counter!("handle_proxy.outbound_request.failed").increment(1);
            warn!("Outbound request to {resp_hostname} timed out after 30s");
            OutboundOutcome::Failed
        }
    }
}

/// Drain remaining outbound futures after the client has already been answered.
async fn finish_remaining_outbounds<F>(mut unordered: FuturesUnordered<F>)
where
    F: std::future::Future<Output = OutboundResult> + Send + 'static,
{
    while let Some(outbound) = unordered.next().await {
        // Body is only needed for connection reuse / metrics; discard after settle.
        let _ = settle_outbound(outbound).await;
    }
}

/// Send a request to its destination, waiting up to OUTBOUND_TIMEOUT for headers.
async fn send_request(
    client: HttpsClient,
    req: Request<Full<Bytes>>,
    request_host: String,
) -> OutboundResult {
    let request_start = Instant::now();
    let response_res = tokio::time::timeout(OUTBOUND_TIMEOUT, client.request(req)).await;
    (response_res, request_host, request_start)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{full, handle_request};
    use crate::{
        config::{ConfigData, KeyRing, OutboundEntry},
        logging::LogFormat,
        state::AppState,
    };
    use http_body_util::{BodyExt, combinators::BoxBody};
    use hyper::{Request, Response, StatusCode, body::Bytes};

    fn make_test_config() -> ConfigData {
        ConfigData {
            sentry_dsn: None,
            sentry_env: None,
            traces_sample_rate: None,
            log_filter: "debug".into(),
            log_format: LogFormat::Text,
            statsd_addr: None,
            default_metrics_tags: None,
            ip: "127.0.0.1".into(),
            port: 3000,
            verbose: true,
            keys: vec![
                KeyRing {
                    inbound: Some(
                        "https://eeeeee12345678901234567890123456@localhost:3000/1234".to_string(),
                    ),
                    outbound: vec![
                        OutboundEntry::Dsn(Some(
                            "https://aaaaaaaa123456789012345678901234@target.example.com/5678"
                                .to_string(),
                        )),
                        OutboundEntry::Dsn(Some(
                            "https://bbbbbbbb234567890123456789012345@other.example.com/9012"
                                .to_string(),
                        )),
                    ],
                },
                KeyRing {
                    inbound: Some(
                        "https://ddddddd1234567890123456789012345@localhost:3000/3456".to_string(),
                    ),
                    outbound: vec![OutboundEntry::Dsn(Some(
                        "https://bbbbbb12345678901234567890123456@target.example.com/7890"
                            .to_string(),
                    ))],
                },
            ],
            modify_envelope_header: true,
        }
    }

    fn make_app_state() -> Arc<AppState> {
        let config = make_test_config();
        let state = AppState::from_config(config);
        Arc::new(state)
    }

    async fn extract_body(response: Response<BoxBody<Bytes, hyper::Error>>) -> String {
        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();

        String::from_utf8(body_bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn test_handle_request_health() {
        let state = make_app_state();
        let builder = Request::builder()
            .method("GET")
            .uri("http://example.com/health");
        let request = builder.body(full("")).unwrap();
        let response_res = handle_request(request, state).await;

        assert!(response_res.is_ok());
        let response = response_res.unwrap();
        assert_eq!(StatusCode::OK, response.status());
        let body = extract_body(response).await;
        assert_eq!("ok", body);
    }

    #[tokio::test]
    async fn test_handle_request_proxy_incorrect_method() {
        let state = make_app_state();
        let builder = Request::builder()
            .method("GET")
            .uri("http://localhost:3000/store");
        let request = builder.body(full("")).unwrap();
        let response_res = handle_request(request, state).await;

        assert!(response_res.is_ok());
        let response = response_res.unwrap();

        assert_eq!(StatusCode::METHOD_NOT_ALLOWED, response.status());
        let body = extract_body(response).await;
        assert_eq!("Method not allowed", body);
    }

    #[tokio::test]
    async fn test_handle_proxy_no_dsn() {
        let state = make_app_state();
        let builder = Request::builder()
            .method("POST")
            .uri("http://localhost:3000/store");

        let request = builder.body(full("")).unwrap();
        let response_res = handle_request(request, state).await;

        assert!(response_res.is_ok());
        let response = response_res.unwrap();

        assert_eq!(StatusCode::BAD_REQUEST, response.status());
        let body = extract_body(response).await;
        assert_eq!("No DSN found", body);
    }

    #[tokio::test]
    async fn test_handle_proxy_incorrect_dsn() {
        let state = make_app_state();
        let builder = Request::builder()
            .method("POST")
            .header("Authorization", "sentry_key=not-there")
            .uri("http://localhost:3000/store");

        let request = builder.body(full("")).unwrap();
        let response_res = handle_request(request, state).await;

        assert!(response_res.is_ok());
        let response = response_res.unwrap();

        assert_eq!(StatusCode::BAD_REQUEST, response.status());
        let body = extract_body(response).await;
        assert_eq!("No DSN found", body);
    }
}
