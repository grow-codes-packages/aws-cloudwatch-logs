//! Embedded Metric Format (EMF) helper.

use std::collections::BTreeMap;
use std::time::SystemTime;

use serde_json::{json, Map, Value};

use crate::client::CloudWatchLogsClient;
use crate::config::OversizePolicy;
use crate::core::{Error, Result};
use crate::models::{to_ms, LogEvent, PutLogEventsOptions};

/// Metric namespace used when [`EmfOptions::namespace`] is not set.
pub const DEFAULT_EMF_NAMESPACE: &str = "GrowSdk";

/// Units CloudWatch metrics accept.
pub const EMF_UNITS: &[&str] = &[
    "Seconds",
    "Microseconds",
    "Milliseconds",
    "Bytes",
    "Kilobytes",
    "Megabytes",
    "Gigabytes",
    "Terabytes",
    "Bits",
    "Kilobits",
    "Megabits",
    "Gigabits",
    "Terabits",
    "Percent",
    "Count",
    "Bytes/Second",
    "Kilobytes/Second",
    "Megabytes/Second",
    "Gigabytes/Second",
    "Terabytes/Second",
    "Bits/Second",
    "Kilobits/Second",
    "Megabits/Second",
    "Gigabits/Second",
    "Terabits/Second",
    "Count/Second",
    "None",
];

/// Options of [`build_emf`] and [`CloudWatchLogsClient::emf`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct EmfOptions {
    /// Metric namespace, 1–1024 characters (default `GrowSdk`).
    pub namespace: Option<String>,
    /// CloudWatch unit from [`EMF_UNITS`] (default `None`).
    pub unit: Option<String>,
    /// Metric time (default now).
    pub timestamp: Option<SystemTime>,
    /// Extra top-level fields (searchable, not dimensions).
    pub properties: BTreeMap<String, Value>,
    /// When not empty, replaces `value` with up to 100 values.
    pub values: Vec<f64>,
    /// Log group `emf` writes to (default: the client's log group).
    pub group: Option<String>,
    /// Log stream `emf` writes to (default: the client's log stream).
    pub stream: Option<String>,
}

impl EmfOptions {
    /// Sets the namespace.
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }
    /// Sets the unit.
    pub fn unit(mut self, unit: impl Into<String>) -> Self {
        self.unit = Some(unit.into());
        self
    }
    /// Adds a property.
    pub fn property(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }
}

/// Builds a CloudWatch Embedded Metric Format document for one metric: `value` (or
/// `opts.values`) is published under `metric` with `dimensions` as one dimension set (at most
/// 30). Returns the JSON text; invalid input is `VALIDATION`. Metric extraction happens inside
/// AWS.
pub fn build_emf(
    metric: &str,
    value: f64,
    dimensions: &BTreeMap<String, String>,
    opts: &EmfOptions,
) -> Result<String> {
    let invalid = |m: &str| Err(Error::validation(format!("emf: {m}")));
    let namespace = opts.namespace.as_deref().unwrap_or(DEFAULT_EMF_NAMESPACE);
    let unit = opts.unit.as_deref().unwrap_or("None");
    if metric.is_empty() || metric.chars().count() > 1024 {
        return invalid("metric name must be 1-1024 characters");
    }
    if namespace.is_empty() || namespace.chars().count() > 1024 {
        return invalid("namespace must be 1-1024 characters");
    }
    if !EMF_UNITS.contains(&unit) {
        return invalid(&format!("unknown unit {unit}"));
    }
    if dimensions.len() > 30 {
        return invalid("at most 30 dimensions");
    }
    let values: Vec<f64> = if opts.values.is_empty() {
        vec![value]
    } else {
        opts.values.clone()
    };
    if values.len() > 100 || values.iter().any(|v| !v.is_finite()) {
        return invalid("value must be a finite number or 1-100 finite numbers");
    }
    let mut doc = Map::new();
    for (k, v) in &opts.properties {
        if k == "_aws" || k == metric {
            return invalid(&format!("property {k} is reserved"));
        }
        doc.insert(k.clone(), v.clone());
    }
    for (k, v) in dimensions {
        if k.is_empty() || k == "_aws" || k == metric {
            return invalid("dimension names must be non-empty and not reserved");
        }
        doc.insert(k.clone(), Value::String(v.clone()));
    }
    let number = |v: f64| {
        serde_json::Number::from_f64(v)
            .map(Value::Number)
            .unwrap_or(Value::Null)
    };
    let metric_value = if opts.values.is_empty() {
        if value.fract() == 0.0 && value.abs() < 9.0e15 {
            json!(value as i64)
        } else {
            number(value)
        }
    } else {
        Value::Array(values.into_iter().map(number).collect())
    };
    doc.insert(metric.to_owned(), metric_value);
    let keys: Vec<&String> = dimensions.keys().collect();
    let ts = to_ms(opts.timestamp.unwrap_or_else(SystemTime::now));
    doc.insert(
        "_aws".to_owned(),
        json!({
            "Timestamp": ts,
            "CloudWatchMetrics": [{
                "Namespace": namespace,
                "Dimensions": [keys],
                "Metrics": [{"Name": metric, "Unit": unit}],
            }],
        }),
    );
    Ok(Value::Object(doc).to_string())
}

impl CloudWatchLogsClient {
    /// Builds an Embedded Metric Format document (see [`build_emf`]), writes it as one event with
    /// `PutLogEvents` to `opts.group` / `opts.stream` (default: the client's log group and log
    /// stream; a missing stream is created) and returns the document sent. `CONFIG` when no log
    /// group is known; `VALIDATION` when the document is oversize or the service rejects the
    /// event.
    pub async fn emf(
        &self,
        metric: &str,
        value: f64,
        dimensions: &BTreeMap<String, String>,
        opts: EmfOptions,
    ) -> Result<String> {
        let group = self.group_or_default("emf", opts.group.as_deref())?;
        let stream = opts
            .stream
            .clone()
            .unwrap_or_else(|| self.log_stream().to_owned());
        let mut opts = opts;
        let ts = *opts.timestamp.get_or_insert_with(SystemTime::now);
        let doc = build_emf(metric, value, dimensions, &opts)?;
        let res = self
            .put_log_events(
                &group,
                &stream,
                vec![LogEvent::at(ts, doc.clone())],
                PutLogEventsOptions {
                    oversize_policy: Some(OversizePolicy::Error),
                    ..Default::default()
                },
            )
            .await?;
        if res.rejected > 0 {
            return Err(Error::validation("emf: the service rejected the event"));
        }
        Ok(doc)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ErrorCode;
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn documents_and_validation() {
        let dims: BTreeMap<String, String> = [("service".to_owned(), "api".to_owned())].into();
        let opts = EmfOptions {
            timestamp: Some(UNIX_EPOCH + Duration::from_millis(5)),
            ..Default::default()
        }
        .namespace("Ns")
        .unit("Milliseconds")
        .property("marker", "m");
        let doc: Value =
            serde_json::from_str(&build_emf("latency", 42.0, &dims, &opts).unwrap()).unwrap();
        assert_eq!(doc["latency"], json!(42));
        assert_eq!(doc["service"], "api");
        assert_eq!(doc["marker"], "m");
        assert_eq!(doc["_aws"]["Timestamp"], 5);
        assert_eq!(doc["_aws"]["CloudWatchMetrics"][0]["Namespace"], "Ns");
        assert_eq!(
            doc["_aws"]["CloudWatchMetrics"][0]["Dimensions"],
            json!([["service"]])
        );
        let doc: Value = serde_json::from_str(
            &build_emf("m", 1.5, &BTreeMap::new(), &EmfOptions::default()).unwrap(),
        )
        .unwrap();
        assert_eq!(doc["m"], 1.5);
        assert_eq!(doc["_aws"]["CloudWatchMetrics"][0]["Namespace"], "GrowSdk");
        let multi = EmfOptions {
            values: vec![1.0, 2.5],
            ..Default::default()
        };
        let doc: Value =
            serde_json::from_str(&build_emf("m", 0.0, &BTreeMap::new(), &multi).unwrap()).unwrap();
        assert_eq!(doc["m"], json!([1.0, 2.5]));

        let bad = |metric: &str, value: f64, dims: &BTreeMap<String, String>, o: &EmfOptions| {
            assert_eq!(
                build_emf(metric, value, dims, o).unwrap_err().code(),
                ErrorCode::Validation
            )
        };
        let none = BTreeMap::new();
        bad("", 1.0, &none, &EmfOptions::default());
        bad("m", f64::NAN, &none, &EmfOptions::default());
        bad("m", 1.0, &none, &EmfOptions::default().namespace(""));
        bad("m", 1.0, &none, &EmfOptions::default().unit("Parsecs"));
        bad("m", 1.0, &none, &EmfOptions::default().property("_aws", 1));
        bad(
            "m",
            1.0,
            &none,
            &EmfOptions {
                values: vec![1.0; 101],
                ..Default::default()
            },
        );
        let many: BTreeMap<String, String> =
            (0..31).map(|i| (format!("d{i}"), "v".to_owned())).collect();
        bad("m", 1.0, &many, &EmfOptions::default());
        let reserved: BTreeMap<String, String> = [("m".to_owned(), "v".to_owned())].into();
        bad("m", 1.0, &reserved, &EmfOptions::default());
    }
}
