use crate::utils::get_missing_collectors;
use axum::extract::{Query, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use axum_prometheus::PrometheusMetricLayerBuilder;
use bgpkit_broker::{BrokerItem, LocalBrokerDb, DEFAULT_PAGE_SIZE};
use chrono::{DateTime, NaiveDate, NaiveDateTime};
use clap::Args;
use futures::stream;
use http::{Method, StatusCode};
use log::error;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tower_http::cors::{Any, CorsLayer};
use tracing::{info, warn};

pub(crate) const LIVE_EVENT_BUFFER_SIZE: usize = 4096;

struct AppState {
    database: LocalBrokerDb,
    live_events: broadcast::Sender<BrokerItem>,
    updater_enabled: bool,
}

#[derive(Args, Debug, Serialize, Deserialize)]
pub struct BrokerSearchQuery {
    /// Start timestamp
    #[clap(short = 't', long)]
    pub ts_start: Option<String>,

    /// End timestamp
    #[clap(short = 'T', long)]
    pub ts_end: Option<String>,

    /// Duration string, e.g. 1 hour
    #[clap(short = 'd', long)]
    pub duration: Option<String>,

    /// filter by route collector projects, i.e. `route-views` or `riperis`
    #[clap(short, long)]
    pub project: Option<String>,

    /// filter by collector IDs, e.g. 'rrc00', 'route-views2. use comma to separate multiple collectors
    #[clap(short, long)]
    pub collector_id: Option<String>,

    /// filter by data types, i.e. 'updates', 'rib'.
    #[clap(short = 'D', long)]
    pub data_type: Option<String>,

    /// page number
    #[clap(long)]
    pub page: Option<usize>,

    /// page size
    #[clap(long)]
    pub page_size: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BrokerHealthQueryParams {
    /// maximum allowed delay in seconds
    pub max_delay_secs: Option<u32>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BrokerSearchResult {
    pub total: usize,
    pub count: usize,
    pub page: usize,
    pub page_size: usize,
    pub error: Option<String>,
    pub data: Vec<BrokerItem>,
    pub meta: Option<Meta>,
}

#[derive(Serialize, Deserialize)]
enum BrokerApiError {
    BrokerNotHealthy(String),
    SearchError(String),
    LiveUpdatesUnavailable(String),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Meta {
    pub latest_update_ts: NaiveDateTime,
    pub latest_update_duration: i32,
}

/// Search MRT files meta data from BGPKIT Broker database
async fn search(
    query: Query<BrokerSearchQuery>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let (page, page_size) = (
        query.page.unwrap_or(1),
        query.page_size.unwrap_or(DEFAULT_PAGE_SIZE),
    );
    if page == 0 {
        return (
            StatusCode::BAD_REQUEST,
            Json(BrokerApiError::SearchError(
                "page number start from 1".to_string(),
            )),
        )
            .into_response();
    }
    if page_size > 1000 {
        return (
            StatusCode::BAD_REQUEST,
            Json(BrokerApiError::SearchError(
                "maximum page size is 1000".to_string(),
            )),
        )
            .into_response();
    }

    let mut ts_start = match &query.ts_start {
        Some(s) => match parse_time_str(s.as_str()) {
            Ok(ts) => Some(ts),
            Err(e) => {
                let err_msg = format!("cannot parse ts_start {}: {}", s, e);
                error!("{}", &err_msg);
                error!("{:?}", &query);
                return (
                    StatusCode::BAD_REQUEST,
                    Json(BrokerApiError::SearchError(err_msg)),
                )
                    .into_response();
            }
        },
        None => None,
    };

    let mut ts_end = match &query.ts_end {
        Some(s) => match parse_time_str(s.as_str()) {
            Ok(ts) => Some(ts),
            Err(e) => {
                let err_msg = format!("cannot parse ts_end {}: {}", s, e);
                error!("{}", &err_msg);
                error!("{:?}", &query);
                return (
                    StatusCode::BAD_REQUEST,
                    Json(BrokerApiError::SearchError(err_msg)),
                )
                    .into_response();
            }
        },
        None => None,
    };

    match (ts_start, ts_end) {
        (Some(start), None) => {
            if let Some(duration_str) = &query.duration {
                match humantime::parse_duration(duration_str.as_str()) {
                    Ok(d) => {
                        if let Ok(duration) = chrono::Duration::from_std(d) {
                            ts_end = Some(start + duration);
                        }
                    }
                    Err(_) => {
                        return (
                            StatusCode::BAD_REQUEST,
                            Json(BrokerApiError::SearchError(format!(
                                "cannot parse time duration string: {}",
                                duration_str
                            ))),
                        )
                            .into_response();
                    }
                }
            }
        }
        (None, Some(end)) => {
            if let Some(duration_str) = &query.duration {
                match humantime::parse_duration(duration_str.as_str()) {
                    Ok(d) => {
                        if let Ok(duration) = chrono::Duration::from_std(d) {
                            ts_start = Some(end - duration);
                        }
                    }
                    Err(_) => {
                        return (
                            StatusCode::BAD_REQUEST,
                            Json(BrokerApiError::SearchError(format!(
                                "cannot parse time duration string: {}",
                                duration_str
                            ))),
                        )
                            .into_response();
                    }
                }
            }
        }
        _ => {}
    };

    let collectors = query
        .collector_id
        .as_ref()
        .map(|s| s.split(',').map(|s| s.trim().to_string()).collect());

    let search_result = match state
        .database
        .search(
            collectors,
            query.project.clone(),
            query.data_type.clone(),
            ts_start,
            ts_end,
            Some(page),
            Some(page_size),
        )
        .await
    {
        Ok(result) => result,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(BrokerApiError::SearchError(format!(
                    "database search failed: {}",
                    e
                ))),
            )
                .into_response();
        }
    };

    let meta = state
        .database
        .get_latest_updates_meta()
        .await
        .unwrap_or_default()
        .and_then(|data| {
            Some(Meta {
                latest_update_ts: chrono::DateTime::from_timestamp(data.update_ts, 0)?.naive_utc(),
                latest_update_duration: data.update_duration,
            })
        });

    Json(BrokerSearchResult {
        total: search_result.total,
        count: search_result.items.len(),
        page,
        page_size,
        error: None,
        data: search_result.items,
        meta,
    })
    .into_response()
}

/// Get the latest MRT files meta information
async fn latest(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let items = state.database.get_latest_files().await;
    let meta = state
        .database
        .get_latest_updates_meta()
        .await
        .unwrap_or_default()
        .and_then(|data| {
            Some(Meta {
                latest_update_ts: chrono::DateTime::from_timestamp(data.update_ts, 0)?.naive_utc(),
                latest_update_duration: data.update_duration,
            })
        });

    Json(BrokerSearchResult {
        total: items.len(),
        count: items.len(),
        page: 0,
        page_size: items.len(),
        error: None,
        data: items,
        meta,
    })
}

/// Return Broker API and database health
async fn health(
    query: Query<BrokerHealthQueryParams>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    match state.database.get_latest_timestamp().await {
        Ok(data) => match data {
            None => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(BrokerApiError::BrokerNotHealthy(
                    "database not bootstrap".to_string(),
                )),
            )
                .into_response(),
            Some(ts) => {
                // data is there, service is ok.
                // this endpoint does not check for data freshness, as there are applications
                // that do not require fresh data (e.g., historical analysis).

                let latest_file_ts = ts.and_utc().timestamp();
                let now_ts = chrono::Utc::now().timestamp();

                if let Some(max_delay) = query.max_delay_secs {
                    if now_ts - latest_file_ts > max_delay as i64 {
                        return (
                            StatusCode::SERVICE_UNAVAILABLE,
                            Json(BrokerApiError::BrokerNotHealthy(format!(
                                "database is not fresh, latest file timestamp: {}, delay: {}s",
                                latest_file_ts,
                                now_ts - latest_file_ts
                            ))),
                        )
                            .into_response();
                    }
                }

                Json(
                    json!({"status": "OK", "message": "database is healthy", "meta": {
                        "latest_file_ts": latest_file_ts,
                        "delay_secs": now_ts - latest_file_ts,
                    }}),
                )
                .into_response()
            }
        },
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(BrokerApiError::BrokerNotHealthy(
                "database connection error".to_string(),
            )),
        )
            .into_response(),
    }
}

async fn missing_collectors(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let latest_items = state.database.get_latest_files().await;
    let missing_collectors = get_missing_collectors(&latest_items);

    match missing_collectors.is_empty() {
        true => (
            StatusCode::OK,
            Json(json!(
                {
                    "status": "OK",
                    "message": "no missing collectors",
                    "missing_collectors": []
                }
            )),
        )
            .into_response(),
        false => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!(
                {
                    "status": "Need action",
                    "message": "have missing collectors",
                    "missing_collectors": missing_collectors
                }
            )),
        )
            .into_response(),
    }
}

async fn events(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    if !state.updater_enabled {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(BrokerApiError::LiveUpdatesUnavailable(
                "live SSE notifications require the updater service in the same process"
                    .to_string(),
            )),
        )
            .into_response();
    }

    Sse::new(live_event_stream(state.live_events.subscribe()))
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response()
}

fn live_event_stream(
    receiver: broadcast::Receiver<BrokerItem>,
) -> impl futures::Stream<Item = Result<Event, Infallible>> {
    stream::unfold(receiver, |mut receiver| async move {
        match receiver.recv().await {
            Ok(item) => {
                let event = Event::default()
                    .event("new_file")
                    .id(item.url.clone())
                    .json_data(&item)
                    .expect("BrokerItem should serialize into SSE event");
                Some((Ok::<Event, Infallible>(event), receiver))
            }
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                warn!(
                    "closing SSE connection after lagging behind by {} events",
                    skipped
                );
                None
            }
            Err(broadcast::error::RecvError::Closed) => None,
        }
    })
}

/// Parse a timestamp string into NaiveDateTime
///
/// The timestamp string can be either unix timestamp or RFC3339 format string (e.g. 2020-01-01T00:00:00Z).
fn parse_time_str(ts_str: &str) -> Result<NaiveDateTime, String> {
    if let Ok(ts_end) = ts_str.parse::<i64>() {
        // it's unix timestamp
        return DateTime::from_timestamp(ts_end, 0)
            .map(|dt| dt.naive_utc())
            .ok_or_else(|| format!("invalid unix timestamp: {}", ts_end));
    }

    if let Ok(d) = NaiveDate::parse_from_str(ts_str, "%Y-%m-%d") {
        // it's a date
        return d
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| format!("invalid date: {}", ts_str));
    }

    if let Ok(t) = DateTime::parse_from_rfc3339(ts_str) {
        // it's a correct RFC3339 time
        return Ok(t.naive_utc());
    }

    if let Ok(t) = DateTime::parse_from_rfc2822(ts_str) {
        // it's a correct RFC2822 time
        return Ok(t.naive_utc());
    }

    // at this point, the input not any valid time string format.
    // we guess it could be a timezone-less time string,
    // so let's remove potential "Z" and add timezone and try again
    let ts_str = ts_str.trim_end_matches('Z').to_string() + "+00:00";
    match DateTime::parse_from_rfc3339(ts_str.as_str()) {
        Ok(t) => Ok(t.naive_utc()),
        Err(_) => Err(format!(
            "Invalid timestamp format: {}, should be either unix timestamp or RFC3339",
            ts_str
        )),
    }
}

pub async fn start_api_service(
    database: LocalBrokerDb,
    live_events: broadcast::Sender<BrokerItem>,
    updater_enabled: bool,
    host: String,
    port: u16,
    root: String,
) -> std::io::Result<()> {
    let (metric_layer, metric_handle) = PrometheusMetricLayerBuilder::new()
        .with_ignore_patterns(&["/metrics"])
        .with_prefix("bgpkit_broker")
        .with_default_metrics()
        .build_pair();
    let state = Arc::new(AppState {
        database,
        live_events,
        updater_enabled,
    });
    let app = Router::new()
        .route("/search", get(search))
        .route("/latest", get(latest))
        .route("/health", get(health))
        .route("/missing_collectors", get(missing_collectors))
        .route("/events", get(events))
        .route("/metrics", get(|| async move { metric_handle.render() }))
        .route("/bgpstream", get(bgpstream))
        .route("/data", get(bgpstream))
        .route("/meta/collectors", get(meta_stub))
        .route("/meta/collectors/{name}", get(meta_stub))
        .route("/meta/projects", get(meta_stub))
        .route("/meta/projects/{name}", get(meta_stub))
        .with_state(state)
        .layer(metric_layer)
        .layer(
            CorsLayer::new()
                .allow_methods([Method::GET, Method::POST])
                .allow_origin(Any),
        );
    info!("Starting API service on {}:{}", host, port);

    let root_app = if root == "/" {
        app
    } else {
        Router::new().nest(root.as_str(), app)
    };

    let socket_str = format!("{}:{}", host, port);
    let listener = tokio::net::TcpListener::bind(socket_str).await?;
    info!("listening on {}", listener.local_addr()?);
    axum::serve(listener, root_app).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use chrono::DateTime;
    use futures::StreamExt;
    use http_body_util::BodyExt;
    use tempfile::tempdir;
    use tower::ServiceExt;

    fn test_item(index: i64) -> BrokerItem {
        BrokerItem {
            ts_start: DateTime::from_timestamp(1_710_000_000 + index, 0)
                .unwrap()
                .naive_utc(),
            ts_end: DateTime::from_timestamp(1_710_000_300 + index, 0)
                .unwrap()
                .naive_utc(),
            collector_id: "route-views2".to_string(),
            data_type: "updates".to_string(),
            url: format!("https://example.com/{}", index),
            rough_size: 100,
            exact_size: 100,
        }
    }

    async fn test_database() -> (tempfile::TempDir, LocalBrokerDb) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sqlite3");
        let database = LocalBrokerDb::new(path.to_str().unwrap()).await.unwrap();
        (dir, database)
    }

    fn test_router(
        database: LocalBrokerDb,
        live_events: broadcast::Sender<BrokerItem>,
        updater_enabled: bool,
        root: &str,
    ) -> Router {
        let state = Arc::new(AppState {
            database,
            live_events,
            updater_enabled,
        });
        let app = Router::new()
            .route("/events", get(events))
            .with_state(state);
        if root == "/" {
            app
        } else {
            Router::new().nest(root, app)
        }
    }

    async fn read_sse_frame(response: axum::response::Response) -> String {
        let frame = response.into_body().frame().await.unwrap().unwrap();
        let bytes = frame.into_data().unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn test_events_endpoint_streams_new_file_notifications() {
        let (_dir, database) = test_database().await;
        let (sender, _) = broadcast::channel(LIVE_EVENT_BUFFER_SIZE);
        let app = test_router(database, sender.clone(), true, "/");
        let request = http::Request::builder()
            .uri("/events")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(http::header::CONTENT_TYPE)
                .unwrap()
                .to_str()
                .unwrap(),
            "text/event-stream"
        );

        let item = test_item(1);
        sender.send(item.clone()).unwrap();

        let frame = read_sse_frame(response).await;
        assert!(frame.contains("event: new_file"));
        assert!(frame.contains(&format!("id: {}", item.url)));
        assert!(frame.contains("data: {"));
        assert!(frame.contains("\"collector_id\":\"route-views2\""));
        assert!(frame.contains(&format!("\"url\":\"{}\"", item.url)));
    }

    #[tokio::test]
    async fn test_events_endpoint_honors_root_path() {
        let (_dir, database) = test_database().await;
        let (sender, _) = broadcast::channel(LIVE_EVENT_BUFFER_SIZE);
        let app = test_router(database, sender, true, "/v3/broker");
        let request = http::Request::builder()
            .uri("/v3/broker/events")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(http::header::CONTENT_TYPE)
                .unwrap()
                .to_str()
                .unwrap(),
            "text/event-stream"
        );
    }

    #[tokio::test]
    async fn test_events_endpoint_returns_503_without_updater() {
        let (_dir, database) = test_database().await;
        let (sender, _) = broadcast::channel(LIVE_EVENT_BUFFER_SIZE);
        let app = test_router(database, sender, false, "/");
        let request = http::Request::builder()
            .uri("/events")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_live_event_stream_closes_when_receiver_lags() {
        let (sender, receiver) = broadcast::channel(1);
        let event_stream = live_event_stream(receiver);
        futures::pin_mut!(event_stream);

        sender.send(test_item(1)).unwrap();
        sender.send(test_item(2)).unwrap();

        assert!(event_stream.next().await.is_none());
    }
}

use serde::de::{self, Deserializer};

fn deserialize_string_or_vec_opt<'de, D>(deserializer: D) -> Result<Option<Vec<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    struct StringOrVec;
    impl<'de> de::Visitor<'de> for StringOrVec {
        type Value = Vec<String>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("string or sequence of strings")
        }
        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(vec![value.to_string()])
        }
        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: de::SeqAccess<'de>,
        {
            let mut values = Vec::new();
            while let Some(value) = seq.next_element()? {
                values.push(value);
            }
            Ok(values)
        }
    }
    deserializer.deserialize_any(StringOrVec).map(Some)
}

#[derive(Debug, Deserialize)]
pub struct BgpstreamQuery {
    #[serde(default)]
    pub human: Option<String>,
    #[serde(rename = "collectors[]", deserialize_with = "deserialize_string_or_vec_opt", default)]
    pub collectors: Option<Vec<String>>,
    #[serde(rename = "projects[]", deserialize_with = "deserialize_string_or_vec_opt", default)]
    pub projects: Option<Vec<String>>,
    #[serde(rename = "types[]", deserialize_with = "deserialize_string_or_vec_opt", default)]
    pub types: Option<Vec<String>>,
    #[serde(rename = "resourceTypes[]", deserialize_with = "deserialize_string_or_vec_opt", default)]
    pub resource_types: Option<Vec<String>>,
    #[serde(rename = "intervals[]", deserialize_with = "deserialize_string_or_vec_opt", default)]
    pub intervals: Option<Vec<String>>,
    #[serde(default)]
    pub minInitialTime: Option<i64>,
    #[serde(default)]
    pub dataAddedSince: Option<i64>,
}



/// Stub for /meta/collectors and /meta/projects — bgpreader calls these before /data
async fn meta_stub() -> impl IntoResponse {
    (StatusCode::OK, Json(serde_json::json!({
        "time": chrono::Utc::now().timestamp(),
        "type": "meta",
        "error": null,
        "queryParameters": {},
        "data": {}
    })))
}

pub async fn bgpstream(
    query: Query<BgpstreamQuery>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    // Map bgpstream params to internal search
    let collectors = query.collectors.clone().unwrap_or_default();
    let projects = query.projects.clone().unwrap_or_default();
    let types = query.types.clone().unwrap_or_default();
    let resource_types = query.resource_types.clone().unwrap_or_default();
    let intervals = query.intervals.clone().unwrap_or_default();
    let min_initial_time = query.minInitialTime;
    let data_added_since = query.dataAddedSince;
    
    // Only support ribs/updates, batch for now
    // Normalize bgpstream "ribs" to broker DB "rib"
    let data_type = types.first().map(|t| if t == "ribs" { "rib".to_string() } else { t.clone() });
    let project = projects.first().cloned();
    let human_flag = query.human.as_deref() == Some("true");
    
    // Parse intervals
    let (interval_ts_start, ts_end) = if let Some(iv) = intervals.first() {
        let parts: Vec<&str> = iv.split(',').collect();
        if parts.len() == 2 {
            let start = parts[0].parse::<i64>().ok();
            let end = parts[1].parse::<i64>().ok();
            (
                // Go back one file duration (15 min updates = 900 seconds) to catch overlapping start
                start.and_then(|s| DateTime::from_timestamp(s - 100, 0).map(|dt| dt.naive_utc())),
                // Go forward one file duration to catch overlapping end
                end.and_then(|e| DateTime::from_timestamp(e + 100, 0).map(|dt| dt.naive_utc())),
            )
        } else {
            (None, None)
        }
    } else {
        (None, None)
    };

    // Use the interval ts_start directly for the DB query (includes its own lookback logic).
    // minInitialTime is applied as a strict post-filter below to exclude already-paginated items.
    let ts_start = interval_ts_start;
    
    let search_result = state
        .database
        .search(
            if !collectors.is_empty() { Some(collectors.clone()) } else { None },
            project.clone(),
            data_type.clone(),
            ts_start,
            ts_end,
            Some(1),
            Some(1000),
        )
        .await;
    
    let mut resources = vec![];
    if let Ok(result) = search_result {
        for item in result.items.into_iter().filter(|item| {
            // Strictly exclude items already seen in previous pagination pages.
            // bgpreader sets minInitialTime = lastInitialTime + duration after each page.
            min_initial_time.map_or(true, |mit| item.ts_start.timestamp() > mit)
        }) {
            let is_rib = item.data_type == "rib";
            // RIBs have ts_end == ts_start in the DB (duration=0), but bgpreader uses
            // initialTime + duration to compute minInitialTime for pagination.
            // With duration=0 it loops forever, so we match CAIDA which returns 120s for all RIBs.
            let duration = if is_rib {
                120i64
            } else {
                (item.ts_end - item.ts_start).num_seconds()
            };
            // bgpreader expects "ribs" (plural) to match the request type
            let type_str = if is_rib { "ribs" } else { &item.data_type };
            resources.push(serde_json::json!({
                "url": item.url,
                "format": "mrt",
                "transport": "file",
                "project": if item.collector_id.starts_with("rrc") { "riperis" } else { "routeviews" },
                "collector": item.collector_id,
                "type": type_str,
                "initialTime": item.ts_start.timestamp(),
                "duration": duration,
                "attr": [],
            }));
        }
    }
    
    let resp = serde_json::json!({
        "time": chrono::Utc::now().timestamp(),
        "type": "data",
        "error": null,
        "queryParameters": {
            "human": human_flag,
            "projects": projects,
            "project": project,
            "collectors": collectors,
            "collector": collectors.first().cloned(),
            "types": types,
            "type": types.first().cloned(),
            "resourceTypes": resource_types,
            "resourceType": resource_types.first().cloned(),
            "minInitialTime": min_initial_time,
            "dataAddedSince": data_added_since,
            "intervals": intervals,
            "debug": {
                "numResources": resources.len(),
                "intervals": intervals,
            }
        },
        "data": { "resources": resources },
    });
    (StatusCode::OK, Json(resp))
}
