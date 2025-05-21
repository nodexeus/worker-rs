use std::{fmt::Write, time::Duration};

use prometheus_client::encoding::{EncodeLabelSet, LabelValueEncoder};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::{family::Family, gauge::Gauge, histogram::Histogram, info::Info};
use prometheus_client::registry::{Registry, Unit};
use std::sync::atomic::{AtomicU64, Ordering};
use lazy_static::lazy_static;

use crate::query::result::{QueryError, QueryResult};

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum WorkerStatus {
    Starting,
    NotRegistered,
    DeprecatedVersion,
    UnsupportedVersion,
    Unreliable,
    Active,
    Offline,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum QueryStatus {
    Ok,
    BadRequest,
    NoAllocation,
    ServerError,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct StatusLabels {
    worker_status: WorkerStatus,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct QueryExecutedLabels {
    status: QueryStatus,
}

lazy_static! {
    static ref STATUS: Family<StatusLabels, Gauge> = Default::default();
    pub static ref CHUNKS_AVAILABLE: Gauge = Default::default();
    pub static ref CHUNKS_DOWNLOADING: Gauge = Default::default();
    pub static ref CHUNKS_PENDING: Gauge = Default::default();
    pub static ref CHUNKS_DOWNLOADED: Counter = Default::default();
    pub static ref CHUNKS_FAILED_DOWNLOAD: Counter = Default::default();
    pub static ref CHUNKS_REMOVED: Counter = Default::default();
    pub static ref STORED_BYTES: Gauge = Default::default();

    static ref QUERY_EXECUTED: Family<QueryExecutedLabels, Counter> = Default::default();
    static ref QUERY_RESULT_SIZE: Histogram = Histogram::new(std::iter::empty());
    static ref READ_CHUNKS: Histogram = Histogram::new(std::iter::empty());
    pub static ref RUNNING_QUERIES: Gauge = Default::default();
    
    // Liveness metrics
    static ref PING_COUNT: Counter = Default::default();
    static ref LAST_PING_TIMESTAMP: AtomicU64 = AtomicU64::new(0);
    static ref PING_INTERVAL: Histogram = Histogram::new(
        [0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 15.0, 20.0, 30.0, 60.0, 120.0]
            .into_iter()
            .map(|s| s * 1000.0) // Convert to milliseconds
            .collect()
    );
    static ref MISSED_PINGS: Counter = Default::default();
}

pub fn set_status(status: WorkerStatus) {
    STATUS.clear();
    STATUS
        .get_or_create(&StatusLabels {
            worker_status: status,
        })
        .set(1);
}

/// Record a successful ping and update metrics
pub fn record_ping() {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    
    let last_ping = LAST_PING_TIMESTAMP.swap(now, Ordering::Relaxed);
    
    if last_ping > 0 {
        let interval = now.saturating_sub(last_ping);
        PING_INTERVAL.observe(interval as f64);
        
        // If the interval is too long, we might have missed some pings
        if interval > 15 {  // 15 seconds is the ping timeout
            let missed = (interval / 10).saturating_sub(1);  // 10s is the expected interval
            if missed > 0 {
                MISSED_PINGS.inc_by(missed);
                warn!(
                    "Missed {} pings (last ping was {}s ago)",
                    missed, interval
                );
            }
        }
    }
    
    PING_COUNT.inc();
}

pub fn query_executed(result: &QueryResult) {
    let (status, result) = match result {
        Ok(result) => (QueryStatus::Ok, Some(result)),
        Err(QueryError::NoAllocation) => (QueryStatus::NoAllocation, None),
        Err(QueryError::NotFound | QueryError::BadRequest(_)) => (QueryStatus::BadRequest, None),
        Err(QueryError::Other(_) | QueryError::ServiceOverloaded) => {
            (QueryStatus::ServerError, None)
        }
    };
    QUERY_EXECUTED
        .get_or_create(&QueryExecutedLabels { status })
        .inc();
    if let Some(result) = result {
        QUERY_RESULT_SIZE.observe(result.data.len() as f64);
        READ_CHUNKS.observe(result.num_read_chunks as f64);
    }
}

pub fn register_metrics(registry: &mut Registry, info: Info<Vec<(String, String)>>) {
    registry.register("worker_info", "Worker info", info);
    registry.register(
        "chunks_available",
        "Number of available chunks",
        CHUNKS_AVAILABLE.clone(),
    );
    registry.register(
        "chunks_downloading",
        "Number of chunks being downloaded",
        CHUNKS_DOWNLOADING.clone(),
    );
    registry.register(
        "chunks_pending",
        "Number of chunks pending download",
        CHUNKS_PENDING.clone(),
    );
    registry.register(
        "chunks_downloaded",
        "Number of chunks downloaded",
        CHUNKS_DOWNLOADED.clone(),
    );
    registry.register(
        "chunks_failed_download",
        "Number of chunks failed to download",
        CHUNKS_FAILED_DOWNLOAD.clone(),
    );
    registry.register(
        "chunks_removed",
        "Number of removed chunks",
        CHUNKS_REMOVED.clone(),
    );
    registry.register_with_unit(
        "used_storage",
        "Total bytes stored in the data directory",
        Unit::Bytes,
        STORED_BYTES.clone(),
    );

    // Query metrics
    registry.register(
        "num_queries_executed",
        "Number of executed queries",
        QUERY_EXECUTED.clone(),
    );
    registry.register_with_unit(
        "query_result_size",
        "(Gzipped) result size of an executed query (bytes)",
        Unit::Bytes,
        QUERY_RESULT_SIZE.clone(),
    );
    registry.register(
        "num_read_chunks",
        "Number of chunks read during query execution",
        READ_CHUNKS.clone(),
    );
    registry.register(
        "running_queries",
        "Current number of queries being executed",
        RUNNING_QUERIES.clone(),
    );
    
    // Liveness metrics
    registry.register(
        "pings_total",
        "Total number of pings sent by the worker",
        PING_COUNT.clone(),
    );
    registry.register_with_unit(
        "ping_interval_ms",
        "Time between consecutive pings in milliseconds",
        Unit::Milliseconds,
        PING_INTERVAL.clone(),
    );
    registry.register(
        "missed_pings_total",
        "Total number of missed pings",
        MISSED_PINGS.clone(),
    );
    
    // Worker status
    registry.register("worker_status", "Status of the worker", STATUS.clone());
    
    // Initialize with starting status
    set_status(WorkerStatus::Starting);
}

impl prometheus_client::encoding::EncodeLabelValue for WorkerStatus {
    fn encode(&self, encoder: &mut LabelValueEncoder) -> Result<(), std::fmt::Error> {
        let status = match self {
            WorkerStatus::Starting => "starting",
            WorkerStatus::NotRegistered => "not_registered",
            WorkerStatus::DeprecatedVersion => "deprecated_version",
            WorkerStatus::UnsupportedVersion => "unsupported_version",
            WorkerStatus::Unreliable => "unreliable",
            WorkerStatus::Active => "active",
            WorkerStatus::Offline => "offline",
        };
        encoder.write_str(status)?;
        Ok(())
    }
}

impl prometheus_client::encoding::EncodeLabelValue for QueryStatus {
    fn encode(&self, encoder: &mut LabelValueEncoder) -> Result<(), std::fmt::Error> {
        let status = match self {
            QueryStatus::Ok => "ok",
            QueryStatus::BadRequest => "bad_request",
            QueryStatus::NoAllocation => "no_allocation",
            QueryStatus::ServerError => "server_error",
        };
        encoder.write_str(status)?;
        Ok(())
    }
}
