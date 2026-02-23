//! Lightweight in-process metrics: atomic counters, gauges, and percentile histograms.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

const MAX_QUERY_TIMES: usize = 1000;

/// A single metric entry returned by [`SwarmMetrics::get_all_metrics`].
#[derive(Debug, Clone)]
pub struct MetricEntry {
    pub name: String,
    pub metric_type: String,
    pub value: String,
    pub labels: String,
}

/// Query execution time histogram summary (p50/p95/p99).
#[derive(Debug, Clone)]
pub struct HistogramSummary {
    pub count: u64,
    pub sum: f64,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
}

impl std::fmt::Display for HistogramSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "sum={:.1} count={} p50={:.1} p95={:.1} p99={:.1}",
            self.sum, self.count, self.p50, self.p95, self.p99,
        )
    }
}

pub struct SwarmMetrics {
    pub queries_submitted: AtomicU64,
    pub queries_completed: AtomicU64,
    pub queries_failed: AtomicU64,
    pub queries_rejected: AtomicU64,

    pub active_queries: AtomicU64,
    pub queued_queries: AtomicU64,

    query_times: Mutex<VecDeque<f64>>,
}

impl SwarmMetrics {
    pub fn new() -> Self {
        SwarmMetrics {
            queries_submitted: AtomicU64::new(0),
            queries_completed: AtomicU64::new(0),
            queries_failed: AtomicU64::new(0),
            queries_rejected: AtomicU64::new(0),
            active_queries: AtomicU64::new(0),
            queued_queries: AtomicU64::new(0),
            query_times: Mutex::new(VecDeque::new()),
        }
    }

    pub fn record_query_submitted(&self) {
        self.queries_submitted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_query_completed(&self, duration_secs: f64) {
        self.queries_completed.fetch_add(1, Ordering::Relaxed);

        if let Ok(mut times) = self.query_times.lock() {
            if times.len() >= MAX_QUERY_TIMES {
                times.pop_front(); // O(1) instead of O(n) with Vec::remove(0)
            }
            times.push_back(duration_secs);
        }
    }

    pub fn record_query_failed(&self) {
        self.queries_failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_query_rejected(&self) {
        self.queries_rejected.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_active_queries(&self, n: u64) {
        self.active_queries.store(n, Ordering::Relaxed);
    }

    pub fn set_queued_queries(&self, n: u64) {
        self.queued_queries.store(n, Ordering::Relaxed);
    }

    pub fn get_all_metrics(&self) -> Vec<MetricEntry> {
        let mut entries = Vec::new();

        entries.push(MetricEntry {
            name: "queries_submitted".to_string(),
            metric_type: "counter".to_string(),
            value: self.queries_submitted.load(Ordering::Relaxed).to_string(),
            labels: String::new(),
        });
        entries.push(MetricEntry {
            name: "queries_completed".to_string(),
            metric_type: "counter".to_string(),
            value: self.queries_completed.load(Ordering::Relaxed).to_string(),
            labels: String::new(),
        });
        entries.push(MetricEntry {
            name: "queries_failed".to_string(),
            metric_type: "counter".to_string(),
            value: self.queries_failed.load(Ordering::Relaxed).to_string(),
            labels: String::new(),
        });
        entries.push(MetricEntry {
            name: "queries_rejected".to_string(),
            metric_type: "counter".to_string(),
            value: self.queries_rejected.load(Ordering::Relaxed).to_string(),
            labels: String::new(),
        });

        entries.push(MetricEntry {
            name: "active_queries".to_string(),
            metric_type: "gauge".to_string(),
            value: self.active_queries.load(Ordering::Relaxed).to_string(),
            labels: String::new(),
        });
        entries.push(MetricEntry {
            name: "queued_queries".to_string(),
            metric_type: "gauge".to_string(),
            value: self.queued_queries.load(Ordering::Relaxed).to_string(),
            labels: String::new(),
        });

        let histogram = self.get_query_time_histogram();
        entries.push(MetricEntry {
            name: "query_execution_time_seconds".to_string(),
            metric_type: "histogram".to_string(),
            value: histogram.to_string(),
            labels: String::new(),
        });

        entries
    }

    pub fn get_query_time_histogram(&self) -> HistogramSummary {
        let times = match self.query_times.lock() {
            Ok(t) => t.clone(),
            Err(_) => {
                return HistogramSummary {
                    count: 0,
                    sum: 0.0,
                    p50: 0.0,
                    p95: 0.0,
                    p99: 0.0,
                };
            }
        };

        if times.is_empty() {
            return HistogramSummary {
                count: 0,
                sum: 0.0,
                p50: 0.0,
                p95: 0.0,
                p99: 0.0,
            };
        }

        let count = times.len() as u64;
        let sum: f64 = times.iter().sum();

        let mut sorted: Vec<f64> = times.into_iter().collect();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let p50 = percentile(&sorted, 50.0);
        let p95 = percentile(&sorted, 95.0);
        let p99 = percentile(&sorted, 99.0);

        HistogramSummary {
            count,
            sum,
            p50,
            p95,
            p99,
        }
    }
}

/// Nearest-rank percentile from a sorted slice.
fn percentile(sorted: &[f64], pct: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    if sorted.len() == 1 {
        return sorted[0];
    }
    let rank = (pct / 100.0 * (sorted.len() as f64 - 1.0)).round() as usize;
    let idx = rank.min(sorted.len() - 1);
    sorted[idx]
}

static SWARM_METRICS: OnceLock<SwarmMetrics> = OnceLock::new();

pub fn instance() -> &'static SwarmMetrics {
    SWARM_METRICS.get_or_init(SwarmMetrics::new)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_metrics_are_zero() {
        let m = SwarmMetrics::new();
        assert_eq!(m.queries_submitted.load(Ordering::Relaxed), 0);
        assert_eq!(m.queries_completed.load(Ordering::Relaxed), 0);
        assert_eq!(m.queries_failed.load(Ordering::Relaxed), 0);
        assert_eq!(m.queries_rejected.load(Ordering::Relaxed), 0);
        assert_eq!(m.active_queries.load(Ordering::Relaxed), 0);
        assert_eq!(m.queued_queries.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn record_query_submitted_increments() {
        let m = SwarmMetrics::new();
        m.record_query_submitted();
        m.record_query_submitted();
        m.record_query_submitted();
        assert_eq!(m.queries_submitted.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn record_query_completed_increments_and_stores_time() {
        let m = SwarmMetrics::new();
        m.record_query_completed(1.5);
        m.record_query_completed(2.5);
        assert_eq!(m.queries_completed.load(Ordering::Relaxed), 2);

        let times = m.query_times.lock().unwrap();
        assert_eq!(times.len(), 2);
        let times_vec: Vec<f64> = times.iter().copied().collect();
        assert!((times_vec[0] - 1.5).abs() < f64::EPSILON);
        assert!((times_vec[1] - 2.5).abs() < f64::EPSILON);
    }

    #[test]
    fn record_query_failed_increments() {
        let m = SwarmMetrics::new();
        m.record_query_failed();
        assert_eq!(m.queries_failed.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn record_query_rejected_increments() {
        let m = SwarmMetrics::new();
        m.record_query_rejected();
        m.record_query_rejected();
        assert_eq!(m.queries_rejected.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn set_active_queries_gauge() {
        let m = SwarmMetrics::new();
        m.set_active_queries(5);
        assert_eq!(m.active_queries.load(Ordering::Relaxed), 5);
        m.set_active_queries(0);
        assert_eq!(m.active_queries.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn set_queued_queries_gauge() {
        let m = SwarmMetrics::new();
        m.set_queued_queries(10);
        assert_eq!(m.queued_queries.load(Ordering::Relaxed), 10);
    }

    #[test]
    fn histogram_empty() {
        let m = SwarmMetrics::new();
        let h = m.get_query_time_histogram();
        assert_eq!(h.count, 0);
        assert!((h.sum - 0.0).abs() < f64::EPSILON);
        assert!((h.p50 - 0.0).abs() < f64::EPSILON);
        assert!((h.p95 - 0.0).abs() < f64::EPSILON);
        assert!((h.p99 - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn histogram_single_value() {
        let m = SwarmMetrics::new();
        m.record_query_completed(3.0);
        let h = m.get_query_time_histogram();
        assert_eq!(h.count, 1);
        assert!((h.sum - 3.0).abs() < f64::EPSILON);
        assert!((h.p50 - 3.0).abs() < f64::EPSILON);
        assert!((h.p95 - 3.0).abs() < f64::EPSILON);
        assert!((h.p99 - 3.0).abs() < f64::EPSILON);
    }

    #[test]
    fn histogram_percentiles() {
        let m = SwarmMetrics::new();
        // Insert 100 values: 1.0, 2.0, ..., 100.0
        for i in 1..=100 {
            m.record_query_completed(i as f64);
        }

        let h = m.get_query_time_histogram();
        assert_eq!(h.count, 100);
        assert!((h.sum - 5050.0).abs() < f64::EPSILON);

        assert!((h.p50 - 50.0).abs() < 1.5);
        assert!((h.p95 - 95.0).abs() < 1.5);
        assert!((h.p99 - 99.0).abs() < 1.5);
    }

    #[test]
    fn histogram_caps_at_max_query_times() {
        let m = SwarmMetrics::new();
        for i in 0..(MAX_QUERY_TIMES + 200) {
            m.record_query_completed(i as f64);
        }

        let times = m.query_times.lock().unwrap();
        assert_eq!(times.len(), MAX_QUERY_TIMES);
        let last = *times.back().unwrap();
        assert!((last - (MAX_QUERY_TIMES as f64 + 199.0)).abs() < f64::EPSILON);
    }

    #[test]
    fn get_all_metrics_returns_expected_entries() {
        let m = SwarmMetrics::new();
        m.record_query_submitted();
        m.record_query_completed(1.0);
        m.set_active_queries(2);

        let entries = m.get_all_metrics();
        assert_eq!(entries.len(), 7);

        let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"queries_submitted"));
        assert!(names.contains(&"queries_completed"));
        assert!(names.contains(&"queries_failed"));
        assert!(names.contains(&"queries_rejected"));
        assert!(names.contains(&"active_queries"));
        assert!(names.contains(&"queued_queries"));
        assert!(names.contains(&"query_execution_time_seconds"));

        for e in &entries {
            match e.name.as_str() {
                "queries_submitted" | "queries_completed" | "queries_failed"
                | "queries_rejected" => {
                    assert_eq!(e.metric_type, "counter");
                }
                "active_queries" | "queued_queries" => {
                    assert_eq!(e.metric_type, "gauge");
                }
                "query_execution_time_seconds" => {
                    assert_eq!(e.metric_type, "histogram");
                }
                _ => panic!("unexpected metric: {}", e.name),
            }
        }

        let submitted = entries.iter().find(|e| e.name == "queries_submitted").unwrap();
        assert_eq!(submitted.value, "1");

        let active = entries.iter().find(|e| e.name == "active_queries").unwrap();
        assert_eq!(active.value, "2");
    }

    #[test]
    fn histogram_summary_display() {
        let h = HistogramSummary {
            count: 10,
            sum: 25.5,
            p50: 2.1,
            p95: 5.3,
            p99: 8.7,
        };
        let s = h.to_string();
        assert!(s.contains("sum=25.5"));
        assert!(s.contains("count=10"));
        assert!(s.contains("p50=2.1"));
        assert!(s.contains("p95=5.3"));
        assert!(s.contains("p99=8.7"));
    }

    #[test]
    fn percentile_helper_edge_cases() {
        assert!((percentile(&[], 50.0) - 0.0).abs() < f64::EPSILON);
        assert!((percentile(&[42.0], 50.0) - 42.0).abs() < f64::EPSILON);
        assert!((percentile(&[42.0], 99.0) - 42.0).abs() < f64::EPSILON);

        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        assert!((percentile(&data, 0.0) - 1.0).abs() < f64::EPSILON);
        assert!((percentile(&data, 100.0) - 5.0).abs() < f64::EPSILON);
    }

    #[test]
    fn singleton_instance_is_accessible() {
        let m = instance();
        let _ = m.get_all_metrics();
    }
}
