#![allow(clippy::expect_used, clippy::unwrap_used)]

//! Retention: raw snapshots 14d, daily aggregates 90d. Pruning aggregates
//! before it deletes, is idempotent, and must not change what rules see.

use std::io::Write;

use chrono::{DateTime, Duration, TimeZone, Utc};
use tempfile::{NamedTempFile, TempDir};
use veille::config::{Config, RetentionConfig};
use veille::domain::{Account, Holding};
use veille::rules::build_context;
use veille::store::{Store, TenantId};

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 8, 18, 22, 0, 0)
        .single()
        .expect("ts")
}

fn account(balance_minor: i64) -> Account {
    Account {
        external_id: "acct-1".into(),
        name: "Checking".into(),
        institution: Some("Test Bank".into()),
        kind: "depository".into(),
        status: "active".into(),
        balance_minor,
        currency: "USD".into(),
    }
}

fn holding(value: Option<i64>) -> Holding {
    Holding {
        account_external_id: "acct-1".into(),
        symbol: "VTI".into(),
        quantity: "2.5".into(),
        market_value_minor: value,
        currency: "USD".into(),
        as_of_date: "2026-08-01".parse().expect("date"),
    }
}

async fn store_with_tenant() -> (TempDir, Store, TenantId) {
    let dir = TempDir::new().expect("tempdir");
    let store = Store::open(&dir.path().join("t.sqlite3"))
        .await
        .expect("store");
    let tenant = store
        .tenants()
        .ensure("alpha", "Alpha")
        .await
        .expect("tenant");
    (dir, store, tenant)
}

/// Two syncs per day for `days` days ending today: morning 1000+d, evening
/// 2000+d (so last != min/max ordering is exercised).
async fn seed(store: &Store, tenant: TenantId, days: i64) {
    for d in (0..days).rev() {
        let day = now() - Duration::days(d);
        let morning = day - Duration::hours(10);
        store
            .insert_account_snapshots(tenant, &[account(1000 + d)], morning)
            .await
            .expect("morning");
        store
            .insert_account_snapshots(tenant, &[account(2000 + d)], day)
            .await
            .expect("evening");
        store
            .insert_holding_snapshots(tenant, &[holding(Some(500 + d))], morning)
            .await
            .expect("h1");
        store
            .insert_holding_snapshots(tenant, &[holding(None)], day)
            .await
            .expect("h2");
    }
}

fn cfg(raw: u32, agg: u32) -> RetentionConfig {
    RetentionConfig {
        raw_days: raw,
        aggregate_days: agg,
    }
}

#[tokio::test]
async fn prune_aggregates_before_delete_and_keeps_recent_raw() {
    let (_d, store, tenant) = store_with_tenant().await;
    seed(&store, tenant, 30).await;
    assert_eq!(store.account_snapshot_count(tenant).await.unwrap(), 60);

    let stats = store
        .prune(tenant, now(), &cfg(14, 90))
        .await
        .expect("prune");

    // Days 0..=14 back are raw (15 days x 2); older 15 days are aggregated.
    assert_eq!(store.account_snapshot_count(tenant).await.unwrap(), 30);
    assert_eq!(stats.account_rows_pruned, 30);
    assert_eq!(stats.holding_rows_pruned, 30);
    let daily = store.account_daily(tenant).await.unwrap();
    assert_eq!(daily.len(), 15);
    let oldest = &daily[0];
    // d = 29: morning 1029, evening 2029 (last).
    assert_eq!(oldest.last_balance_minor, 2029);
    assert_eq!(oldest.min_balance_minor, 1029);
    assert_eq!(oldest.max_balance_minor, 2029);
    assert_eq!(oldest.sample_count, 2);
    let hold = store.holding_daily(tenant).await.unwrap();
    assert_eq!(hold.len(), 15);
    // last observation had NULL market value; min/max ignore NULLs.
    assert_eq!(hold[0].last_market_value_minor, None);
    assert_eq!(hold[0].min_market_value_minor, Some(529));
    assert_eq!(hold[0].max_market_value_minor, Some(529));
}

#[tokio::test]
async fn prune_is_idempotent() {
    let (_d, store, tenant) = store_with_tenant().await;
    seed(&store, tenant, 30).await;
    store.prune(tenant, now(), &cfg(14, 90)).await.unwrap();
    let before = store.account_daily(tenant).await.unwrap();
    let again = store.prune(tenant, now(), &cfg(14, 90)).await.unwrap();
    assert_eq!(again.account_rows_pruned, 0);
    assert_eq!(again.holding_rows_pruned, 0);
    assert_eq!(again.aggregate_rows_expired, 0);
    assert_eq!(store.account_daily(tenant).await.unwrap(), before);
    assert_eq!(store.account_snapshot_count(tenant).await.unwrap(), 30);
}

#[tokio::test]
async fn late_row_for_an_aggregated_day_is_merged_not_lost() {
    let (_d, store, tenant) = store_with_tenant().await;
    seed(&store, tenant, 30).await;
    store.prune(tenant, now(), &cfg(14, 90)).await.unwrap();
    // Backdated observation lands on an already aggregated day (d = 29), later
    // than the previous last of that day, with a new max.
    let late = now() - Duration::days(29) + Duration::hours(1);
    store
        .insert_account_snapshots(tenant, &[account(9999)], late)
        .await
        .unwrap();
    store.prune(tenant, now(), &cfg(14, 90)).await.unwrap();
    let daily = store.account_daily(tenant).await.unwrap();
    assert_eq!(daily.len(), 15);
    assert_eq!(daily[0].last_balance_minor, 9999);
    assert_eq!(daily[0].max_balance_minor, 9999);
    assert_eq!(daily[0].min_balance_minor, 1029);
    assert_eq!(daily[0].sample_count, 3);
}

#[tokio::test]
async fn aggregates_expire_after_the_aggregate_window() {
    let (_d, store, tenant) = store_with_tenant().await;
    seed(&store, tenant, 40).await;
    store.prune(tenant, now(), &cfg(14, 30)).await.unwrap();
    let daily = store.account_daily(tenant).await.unwrap();
    // Aggregates kept for days 15..=30 back.
    assert_eq!(daily.len(), 16);
    assert_eq!(daily[0].day, (now() - Duration::days(30)).date_naive());
}

#[tokio::test]
async fn prune_is_tenant_scoped() {
    let (_d, store, alpha) = store_with_tenant().await;
    let beta = store.tenants().ensure("beta", "Beta").await.unwrap();
    seed(&store, alpha, 30).await;
    seed(&store, beta, 30).await;
    store.prune(alpha, now(), &cfg(14, 90)).await.unwrap();
    assert_eq!(store.account_snapshot_count(beta).await.unwrap(), 60);
    assert!(store.account_daily(beta).await.unwrap().is_empty());
}

#[tokio::test]
async fn rule_inputs_are_unchanged_by_pruning() {
    let (_d, store, tenant) = store_with_tenant().await;
    // 80 days: inside the 90-day aggregate window, beyond the 14-day raw one,
    // covering the 60-day balance_band and dormant windows.
    seed(&store, tenant, 80).await;
    let thresholds = veille::config::RuleThresholds::default();
    let before = build_context(&store, tenant, thresholds.clone(), now())
        .await
        .unwrap();
    store.prune(tenant, now(), &cfg(14, 90)).await.unwrap();
    assert!(store.account_snapshot_count(tenant).await.unwrap() < 160);
    let after = build_context(&store, tenant, thresholds, now())
        .await
        .unwrap();
    assert_eq!(before.balances, after.balances);
    assert_eq!(before.account_names, after.account_names);
    assert_eq!(before.account_currencies, after.account_currencies);
    assert_eq!(before.account_kinds, after.account_kinds);
    assert_eq!(
        veille::rules::run_rules(&before),
        veille::rules::run_rules(&after)
    );
}

#[tokio::test]
async fn digest_is_unchanged_by_pruning() {
    let (_d, store, tenant) = store_with_tenant().await;
    seed(&store, tenant, 80).await;
    let before = veille::digest::build_digest_input(&store, tenant, "Alpha", 30, now())
        .await
        .unwrap();
    store.prune(tenant, now(), &cfg(14, 90)).await.unwrap();
    let after = veille::digest::build_digest_input(&store, tenant, "Alpha", 30, now())
        .await
        .unwrap();
    assert_eq!(before, after);
}

#[tokio::test]
async fn migration_0003_preserves_existing_snapshots() {
    // A store already holding data (as a deployed v1 would) keeps every row
    // after reopening: the new migration is additive only.
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("t.sqlite3");
    {
        let store = Store::open(&path).await.unwrap();
        let tenant = store.tenants().ensure("alpha", "Alpha").await.unwrap();
        seed(&store, tenant, 5).await;
    }
    let store = Store::open(&path).await.unwrap();
    let tenant = store.tenants().by_slug("alpha").await.unwrap();
    assert_eq!(store.account_snapshot_count(tenant).await.unwrap(), 10);
    assert!(store.account_daily(tenant).await.unwrap().is_empty());
}

// ---- config ----

const BASE: &str = r#"[[tenants]]
slug = "jd"
display_name = "Self"
[tenants.upstream]
base_url = "http://a:3000"
api_key_env = "K1"
"#;

fn load(extra: &str, env: &[(&str, &str)]) -> Result<Config, veille::config::ConfigError> {
    let mut f = NamedTempFile::new().expect("tempfile");
    f.write_all(format!("store_path = \"/tmp/v.sqlite3\"\n{extra}\n{BASE}").as_bytes())
        .expect("write");
    let env: Vec<(String, String)> = env
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    Config::load_with_env(f.path(), &|k| {
        env.iter().find(|(ek, _)| ek == k).map(|(_, v)| v.clone())
    })
}

#[test]
fn retention_defaults_to_14_and_90() {
    let c = load("", &[]).unwrap();
    assert_eq!(
        c.retention,
        RetentionConfig {
            raw_days: 14,
            aggregate_days: 90
        }
    );
}

#[test]
fn retention_toml_and_env_override() {
    let c = load("[retention]\nraw_days = 7\naggregate_days = 100", &[]).unwrap();
    assert_eq!(c.retention, cfg(7, 100));
    let c = load(
        "[retention]\nraw_days = 7\naggregate_days = 100",
        &[
            ("VEILLE_RETENTION_RAW_DAYS", "10"),
            ("VEILLE_RETENTION_AGGREGATE_DAYS", "120"),
        ],
    )
    .unwrap();
    assert_eq!(c.retention, cfg(10, 120));
}

#[test]
fn retention_rejects_bad_values_with_clear_errors() {
    let e = |extra: &str, env: &[(&str, &str)]| load(extra, env).unwrap_err().to_string();
    assert!(e("[retention]\nraw_days = 0", &[]).contains("raw_days"));
    assert!(e("", &[("VEILLE_RETENTION_AGGREGATE_DAYS", "0")]).contains("aggregate_days"));
    assert!(e("", &[("VEILLE_RETENTION_RAW_DAYS", "abc")]).contains("VEILLE_RETENTION_RAW_DAYS"));
    assert!(e("", &[("VEILLE_RETENTION_RAW_DAYS", "-3")]).contains("VEILLE_RETENTION_RAW_DAYS"));
    // aggregates window shorter than raw window
    let msg = e("[retention]\nraw_days = 30\naggregate_days = 20", &[]);
    assert!(
        msg.contains("aggregate_days") && msg.contains("raw_days"),
        "{msg}"
    );
}

#[test]
fn retention_must_cover_the_longest_rule_window() {
    // Default rules use a 60-day window: 59 days of aggregates would starve them.
    let msg = load("[retention]\naggregate_days = 59", &[])
        .unwrap_err()
        .to_string();
    assert!(msg.contains("60"), "{msg}");
    assert!(load("[retention]\naggregate_days = 60", &[]).is_ok());
    // Derived from config, not hardcoded.
    let mut f = NamedTempFile::new().unwrap();
    f.write_all(
        format!("store_path = \"/tmp/v.sqlite3\"\n[retention]\naggregate_days = 90\n{BASE}\n[tenants.rules]\nbalance_band_window_days = 120")
            .as_bytes(),
    )
    .unwrap();
    let msg = Config::load_with_env(f.path(), &|_| None)
        .unwrap_err()
        .to_string();
    assert!(msg.contains("120"), "{msg}");
}

#[tokio::test]
async fn balance_floor_key_is_stable_after_pruning_a_long_breach() {
    use veille::rules::evaluate_tenant;
    let (_d, store, tenant) = store_with_tenant().await;
    // 120 days continuously below the floor: longer than the 90d aggregates.
    seed(&store, tenant, 120).await;
    let thresholds = veille::config::RuleThresholds {
        balance_floor_minor: Some(50_000),
        ..Default::default()
    };

    let first = evaluate_tenant(&store, tenant, thresholds.clone(), now(), true)
        .await
        .unwrap();
    let key = |fs: &[veille::domain::Finding]| {
        fs.iter()
            .find(|f| f.rule_id == "balance-floor")
            .map(|f| f.dedupe_key.clone())
            .expect("balance-floor finding")
    };
    let original = key(&first);
    let findings_before = store.finding_count(tenant).await.unwrap();

    // Prune daily for a few days; the visible series start moves forward.
    for d in 1..=3 {
        let later = now() + Duration::days(d);
        store
            .insert_account_snapshots(tenant, &[account(1500)], later)
            .await
            .unwrap();
        store.prune(tenant, later, &cfg(14, 90)).await.unwrap();
        let fs = evaluate_tenant(&store, tenant, thresholds.clone(), later, true)
            .await
            .unwrap();
        assert_eq!(key(&fs), original, "key must not drift on day +{d}");
    }
    let new_alerts = store.finding_count(tenant).await.unwrap() - findings_before;
    // Only other rules' findings may appear; no second balance-floor row.
    let keys = store
        .finding_dedupe_keys(tenant, "balance-floor", now() + Duration::days(10))
        .await
        .unwrap();
    let keys: Vec<String> = keys.into_iter().map(|(k, _)| k).collect();
    assert_eq!(keys, vec![original], "new findings: {new_alerts}");
}

#[tokio::test]
async fn prune_rejects_retention_that_overflows_dates() {
    let (_d, store, tenant) = store_with_tenant().await;
    let err = store
        .prune(tenant, now(), &cfg(14, u32::MAX))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("out of range"), "{err}");
}

#[test]
fn retention_rejects_absurdly_large_windows() {
    let msg = load("", &[("VEILLE_RETENTION_AGGREGATE_DAYS", "4000000000")])
        .unwrap_err()
        .to_string();
    assert!(msg.contains("at most"), "{msg}");
}

#[tokio::test]
async fn raised_floor_after_an_old_recovered_breach_alerts_as_a_new_episode() {
    use veille::rules::evaluate_tenant;
    let (_d, store, tenant) = store_with_tenant().await;
    // Old breach of a 500 floor (balance 400), recovered to 800 afterwards.
    for d in (100..=120).rev() {
        store
            .insert_account_snapshots(tenant, &[account(400)], now() - Duration::days(d))
            .await
            .unwrap();
    }
    let old = veille::config::RuleThresholds {
        balance_floor_minor: Some(500),
        ..Default::default()
    };
    evaluate_tenant(&store, tenant, old, now() - Duration::days(100), true)
        .await
        .unwrap();
    for d in (0..100).rev() {
        store
            .insert_account_snapshots(tenant, &[account(800)], now() - Duration::days(d))
            .await
            .unwrap();
    }
    store.prune(tenant, now(), &cfg(14, 90)).await.unwrap();
    // Owner raises the floor: every visible point is now below it.
    let raised = veille::config::RuleThresholds {
        balance_floor_minor: Some(1000),
        ..Default::default()
    };
    let findings = evaluate_tenant(&store, tenant, raised, now(), true)
        .await
        .unwrap();
    let new_key = findings
        .iter()
        .find(|f| f.rule_id == "balance-floor")
        .expect("alert for the raised floor")
        .dedupe_key
        .clone();
    let keys = store
        .finding_dedupe_keys(tenant, "balance-floor", now())
        .await
        .unwrap();
    assert_eq!(
        keys.len(),
        2,
        "a new episode row, not a swallowed duplicate"
    );
    assert!(keys.iter().any(|(k, _)| *k == new_key));
    assert!(!new_key.contains("2026-04"), "{new_key}"); // not the old start
}

#[tokio::test]
async fn raised_floor_after_breach_recovered_inside_window_alerts() {
    use veille::rules::evaluate_tenant;
    let (_d, store, tenant) = store_with_tenant().await;
    // Breach of 500 from day -120 to day -85 (evaluated daily), then recovered to 800.
    for d in (85..=120).rev() {
        store
            .insert_account_snapshots(tenant, &[account(400)], now() - Duration::days(d))
            .await
            .unwrap();
        let old = veille::config::RuleThresholds {
            balance_floor_minor: Some(500),
            ..Default::default()
        };
        evaluate_tenant(&store, tenant, old, now() - Duration::days(d), true)
            .await
            .unwrap();
    }
    for d in (0..85).rev() {
        store
            .insert_account_snapshots(tenant, &[account(800)], now() - Duration::days(d))
            .await
            .unwrap();
    }
    store.prune(tenant, now(), &cfg(14, 90)).await.unwrap();
    let raised = veille::config::RuleThresholds {
        balance_floor_minor: Some(1000),
        ..Default::default()
    };
    let before = store
        .finding_dedupe_keys(tenant, "balance-floor", now())
        .await
        .unwrap();
    let f = evaluate_tenant(&store, tenant, raised, now(), true)
        .await
        .unwrap();
    let k = f
        .iter()
        .find(|f| f.rule_id == "balance-floor")
        .unwrap()
        .dedupe_key
        .clone();
    let after = store
        .finding_dedupe_keys(tenant, "balance-floor", now())
        .await
        .unwrap();
    assert_eq!(
        after.len(),
        before.len() + 1,
        "raised floor alert swallowed: key {k}"
    );
}

#[tokio::test]
async fn corrupt_finding_row_does_not_fail_evaluation() {
    use veille::rules::evaluate_tenant;
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("t.sqlite3");
    let store = Store::open(&path).await.unwrap();
    let tenant = store.tenants().ensure("alpha", "Alpha").await.unwrap();
    seed(&store, tenant, 5).await;
    let pool = sqlx::SqlitePool::connect(&format!("sqlite://{}", path.display()))
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO finding (tenant_id, rule_id, severity, subject, summary, evidence, \
         detected_at, last_seen_at, dedupe_key) \
         SELECT id, 'balance-floor', 'alert', 's', 's', '{}', 'garbage', 'garbage', \
         'balance-floor:acct-1:2026-01-01' FROM tenant WHERE slug = 'alpha'",
    )
    .execute(&pool)
    .await
    .unwrap();
    let thresholds = veille::config::RuleThresholds {
        balance_floor_minor: Some(50_000),
        ..Default::default()
    };
    let findings = evaluate_tenant(&store, tenant, thresholds, now(), false)
        .await
        .expect("evaluation survives a corrupt row");
    assert!(findings.iter().any(|f| f.rule_id == "balance-floor"));
}

#[tokio::test]
async fn raised_floor_yields_one_new_key_not_one_per_day() {
    use veille::rules::evaluate_tenant;
    let (_d, store, tenant) = store_with_tenant().await;
    let at = |t: i64| now() + Duration::days(t);
    let mut keys_at_raise = 0;
    for t in 0..=230 {
        // 600 for days 0-99, then 400; floor 500 until day 131, then 1000.
        let balance = if t < 100 { 600 } else { 400 };
        store
            .insert_account_snapshots(tenant, &[account(balance)], at(t))
            .await
            .unwrap();
        store.prune(tenant, at(t), &cfg(14, 90)).await.unwrap();
        let floor = if t < 131 { 500 } else { 1000 };
        let thresholds = veille::config::RuleThresholds {
            balance_floor_minor: Some(floor),
            ..Default::default()
        };
        if t >= 100 {
            evaluate_tenant(&store, tenant, thresholds, at(t), true)
                .await
                .unwrap();
        }
        if t == 130 {
            keys_at_raise = store
                .finding_dedupe_keys(tenant, "balance-floor", at(t))
                .await
                .unwrap()
                .len();
        }
    }
    let keys = store
        .finding_dedupe_keys(tenant, "balance-floor", at(231))
        .await
        .unwrap();
    assert_eq!(keys_at_raise, 1);
    assert!(
        keys.len() <= keys_at_raise + 1,
        "raised floor must add at most one key, got {keys:?}"
    );
}
