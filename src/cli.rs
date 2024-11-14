use std::time::Duration;

use anyhow::Result;
use camino::Utf8PathBuf as PathBuf;
use clap::Parser;
use sqd_network_transport::{PeerId, TransportArgs};

#[derive(Parser, Clone)]
#[command(version)]
pub struct Args {
    /// Directory to keep in the data and state of this worker (defaults to cwd)
    #[clap(
        long,
        env,
        value_name = "DIR",
        default_value = ".",
        hide_default_value(true)
    )]
    pub data_dir: PathBuf,

    /// Port to listen on
    #[clap(short, long, env, default_value_t = 8000, alias = "port")]
    pub http_port: u16,

    #[clap(env, default_value_t = 20)]
    pub parallel_queries: usize,

    #[clap(env, default_value_t = 3)]
    pub concurrent_downloads: usize,

    #[clap(env)]
    pub query_threads: Option<usize>,

    #[clap(env = "PING_INTERVAL_SEC", hide(true), value_parser=parse_seconds, default_value = "55", alias = "ping-interval")]
    pub heartbeat_interval: Duration,
    /// Peer ID of the scheduler
    #[clap(long, env)]
    pub scheduler_id: PeerId,

    /// Peer ID of the logs collector
    #[clap(long, env)]
    pub logs_collector_id: PeerId,

    #[clap(env = "NETWORK_POLLING_INTERVAL_SEC", hide(true), value_parser=parse_seconds, default_value = "30"
    )]
    pub network_polling_interval: Duration,

    #[clap(env = "ASSIGNMENT_CHECK_INTERVAL_SEC", hide(true), value_parser=parse_seconds, default_value = "60")]
    pub assignment_check_interval: Duration,

    #[command(flatten)]
    pub transport: TransportArgs,

    #[clap(env, hide(true))]
    pub sentry_dsn: Option<String>,

    #[clap(env, hide(true), default_value_t = 0.001)]
    pub sentry_traces_sample_rate: f32,
}

fn parse_seconds(s: &str) -> Result<Duration> {
    Ok(Duration::from_secs(s.parse()?))
}
