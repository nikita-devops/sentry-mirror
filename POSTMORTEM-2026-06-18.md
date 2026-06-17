# Postmortem: Outbound coupling via `join_all`

**Date:** 2026-06-18
**Service:** sentry-mirror
**Duration:** ~30m (to detection and fix)

## Summary

The application stopped forwarding events to all outbound targets when one outbound server became unresponsive (accepted TCP connections but never sent data — connections stuck in `CLOSE_WAIT`). Due to an architectural bug, this blocked forwarding to all other, independent, outbound targets as well.

## Timeline

1. Outbound server A (problematic) started accepting TCP connections but never responded — all connections from sentry-mirror to it went into `CLOSE_WAIT`
2. `join_all` in `handle_proxy` waited for **all** outbound futures, including the one stuck on server A
3. The client response was not sent until **all** outbound futures completed → server A never completed → handler hung forever
4. Clients (Sentry SDK/Relay) received no response → timed out and retried → connections piled up
5. Outbound B (healthy) could not deliver events either, because `join_all` never yielded control until server A completed

## Root Cause

**File:** `src/service.rs:203` (before fix)

The original code used `futures::future::join_all` to wait for all outbound requests:

```rust
for (resp_future, resp_hostname, request_start) in join_all(responses).await {
```

Issues:
1. **`join_all` waits for ALL futures** — if any single outbound hangs, the handler never returns a response. This creates head-of-line blocking: healthy outbounds cannot deliver their result to the client.
2. **Response body not drained** for non-primary successful responses — on `continue` (when `found_body = true`) the response body was dropped without being read, which could prevent proper connection reuse in the pool and exacerbate `CLOSE_WAIT`.
3. **No timeouts** — the HTTP client was created without `pool_idle_timeout` and without any response timeout. A hanging outbound would hang forever.

## Fix

### 1. `join_all` → `FuturesUnordered` + `break` on first success

```rust
let mut unordered: FuturesUnordered<_> = responses.into_iter().collect();
// ...
while let Some((resp_future, resp_hostname, request_start)) = unordered.next().await {
    let response_res = tokio::time::timeout(OUTBOUND_TIMEOUT, resp_future).await;
    match response_res {
        Ok(Ok(response)) => {
            if !resp_body.is_empty() {
                // Drain in background — don't block other outbounds
                tokio::spawn(async move { let _ = response.collect().await; });
                continue;
            }
            if let Ok(response_body) = response.collect().await {
                resp_body = response_body.to_bytes();
                break;  // ← Return to client immediately
            }
        }
        Ok(Err(e)) => { /* log */ }
        Err(_) => { /* timeout — log */ }
    }
}
```

**What changes:**
- `FuturesUnordered` yields results as they complete (does not wait for all)
- `break` on the first successful response — the client gets an answer immediately
- Remaining futures are dropped when `unordered` goes out of scope; hyper properly closes the connections
- For responses received after the first success — the body is drained in `tokio::spawn` so the connection pool can reuse the connection

### 2. Response timeout (30s)

Each outbound request is wrapped in `tokio::time::timeout(Duration::from_secs(30), ...)`. If a server does not respond within 30 seconds, the future completes with a timeout error.

### 3. `pool_idle_timeout` (30s)

```rust
Client::builder(TokioExecutor::new())
    .pool_idle_timeout(Duration::from_secs(30))
    .build::<_, Full<Bytes>>(https);
```

Idle connections in the pool are now closed after 30 seconds, preventing `CLOSE_WAIT` accumulation.

### 4. Improved logging

- Client peer address on `IncompleteMessage` (`main.rs:77`)
- Method/path and error when `read_and_decode_body` fails (`service.rs:137`)
- Warning when no outbound requests are made (`service.rs:197`)
- Outbound timeouts are logged separately (`service.rs:243`)

## Impact

- Clients received no response → retries → cascading load increase
- Outbound B (healthy) could not deliver events because the handler never yielded control
- After fix: outbounds are fully independent — a problem with one does not affect others

## Action Items

- [x] Replace `join_all` with `FuturesUnordered` + `break` on first success
- [x] Add 30s response timeout
- [x] Add 30s pool idle timeout
- [x] Drain unused response bodies in background
- [x] Improve error logging
