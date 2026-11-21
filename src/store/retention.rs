//! Retention: roll raw snapshot rows older than the raw window into daily
//! aggregates, delete them, and expire aggregates past the aggregate window.
//!
//! Everything for one tenant runs in a single transaction, so a failure
//! leaves raw data intact. Aggregation is keyed on the entity + UTC day with
//! a merge-on-conflict, which makes a re-run (or a late row for an already
//! aggregated day) lose nothing. Dates are compared as `YYYY-MM-DD` text:
//! `as_of` is stored RFC 3339 UTC, so its first ten characters are the day.

use chrono::{DateTime, Duration, NaiveDate, Utc};
use sqlx::Row;

use super::{Result, Store, StoreError, TenantId};
use crate::config::RetentionConfig;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PruneStats {
    pub account_rows_pruned: u64,
    pub holding_rows_pruned: u64,
    pub aggregate_rows_expired: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountDailyRow {
    pub external_id: String,
    pub day: NaiveDate,
    pub last_balance_minor: i64,
    pub min_balance_minor: i64,
    pub max_balance_minor: i64,
    pub sample_count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HoldingDailyRow {
    pub account_external_id: String,
    pub symbol: String,
    pub day: NaiveDate,
    pub last_market_value_minor: Option<i64>,
    pub min_market_value_minor: Option<i64>,
    pub max_market_value_minor: Option<i64>,
    pub sample_count: i64,
}

const AGGREGATE_ACCOUNTS: &str = "\
WITH ranked AS (
  SELECT *, substr(as_of, 1, 10) AS d,
    ROW_NUMBER() OVER w AS rn,
    MIN(balance_minor) OVER p AS mn,
    MAX(balance_minor) OVER p AS mx,
    COUNT(*) OVER p AS n
  FROM account_snapshot
  WHERE tenant_id = ?1 AND substr(as_of, 1, 10) < ?2
  WINDOW p AS (PARTITION BY external_id, substr(as_of, 1, 10)),
         w AS (PARTITION BY external_id, substr(as_of, 1, 10) ORDER BY as_of DESC, id DESC)
)
INSERT INTO account_snapshot_daily
  (tenant_id, external_id, day, name, kind, currency,
   last_balance_minor, min_balance_minor, max_balance_minor, last_as_of, sample_count)
SELECT tenant_id, external_id, d, name, kind, currency, balance_minor, mn, mx, as_of, n
FROM ranked WHERE rn = 1
ON CONFLICT (tenant_id, external_id, day) DO UPDATE SET
  name = CASE WHEN excluded.last_as_of >= last_as_of THEN excluded.name ELSE name END,
  kind = CASE WHEN excluded.last_as_of >= last_as_of THEN excluded.kind ELSE kind END,
  currency = CASE WHEN excluded.last_as_of >= last_as_of THEN excluded.currency ELSE currency END,
  last_balance_minor = CASE WHEN excluded.last_as_of >= last_as_of
                            THEN excluded.last_balance_minor ELSE last_balance_minor END,
  min_balance_minor = MIN(min_balance_minor, excluded.min_balance_minor),
  max_balance_minor = MAX(max_balance_minor, excluded.max_balance_minor),
  last_as_of = MAX(last_as_of, excluded.last_as_of),
  sample_count = sample_count + excluded.sample_count";

const AGGREGATE_HOLDINGS: &str = "\
WITH ranked AS (
  SELECT *, substr(as_of, 1, 10) AS d,
    ROW_NUMBER() OVER w AS rn,
    MIN(market_value_minor) OVER p AS mn,
    MAX(market_value_minor) OVER p AS mx,
    COUNT(*) OVER p AS n
  FROM holding_snapshot
  WHERE tenant_id = ?1 AND substr(as_of, 1, 10) < ?2
  WINDOW p AS (PARTITION BY account_external_id, symbol, substr(as_of, 1, 10)),
         w AS (PARTITION BY account_external_id, symbol, substr(as_of, 1, 10)
               ORDER BY as_of DESC, id DESC)
)
INSERT INTO holding_snapshot_daily
  (tenant_id, account_external_id, symbol, day, quantity, currency, position_date,
   last_market_value_minor, min_market_value_minor, max_market_value_minor,
   last_as_of, sample_count)
SELECT tenant_id, account_external_id, symbol, d, quantity, currency, position_date,
       market_value_minor, mn, mx, as_of, n
FROM ranked WHERE rn = 1
ON CONFLICT (tenant_id, account_external_id, symbol, day) DO UPDATE SET
  quantity = CASE WHEN excluded.last_as_of >= last_as_of THEN excluded.quantity ELSE quantity END,
  currency = CASE WHEN excluded.last_as_of >= last_as_of THEN excluded.currency ELSE currency END,
  position_date = CASE WHEN excluded.last_as_of >= last_as_of
                       THEN excluded.position_date ELSE position_date END,
  last_market_value_minor = CASE WHEN excluded.last_as_of >= last_as_of
                                 THEN excluded.last_market_value_minor
                                 ELSE last_market_value_minor END,
  min_market_value_minor = CASE WHEN min_market_value_minor IS NULL THEN excluded.min_market_value_minor
                                WHEN excluded.min_market_value_minor IS NULL THEN min_market_value_minor
                                ELSE MIN(min_market_value_minor, excluded.min_market_value_minor) END,
  max_market_value_minor = CASE WHEN max_market_value_minor IS NULL THEN excluded.max_market_value_minor
                                WHEN excluded.max_market_value_minor IS NULL THEN max_market_value_minor
                                ELSE MAX(max_market_value_minor, excluded.max_market_value_minor) END,
  last_as_of = MAX(last_as_of, excluded.last_as_of),
  sample_count = sample_count + excluded.sample_count";

fn day_cutoff(now: DateTime<Utc>, days: u32) -> String {
    (now.date_naive() - Duration::days(i64::from(days)))
        .format("%Y-%m-%d")
        .to_string()
}

fn parse_day(raw: &str, column: &'static str) -> Result<NaiveDate> {
    raw.parse().map_err(|e: chrono::ParseError| {
        StoreError::Db(sqlx::Error::Decode(format!("corrupt {column}: {e}").into()))
    })
}

impl Store {
    /// Aggregate-then-delete for one tenant. Raw rows on days strictly older
    /// than `now - raw_days` become daily aggregates; aggregates on days
    /// strictly older than `now - aggregate_days` are deleted. Atomic and
    /// safe to re-run.
    pub async fn prune(
        &self,
        tenant: TenantId,
        now: DateTime<Utc>,
        retention: &RetentionConfig,
    ) -> Result<PruneStats> {
        let raw_cutoff = day_cutoff(now, retention.raw_days);
        let agg_cutoff = day_cutoff(now, retention.aggregate_days);
        let mut tx = self.pool().begin().await?;

        sqlx::query(AGGREGATE_ACCOUNTS)
            .bind(tenant.0)
            .bind(&raw_cutoff)
            .execute(&mut *tx)
            .await?;
        sqlx::query(AGGREGATE_HOLDINGS)
            .bind(tenant.0)
            .bind(&raw_cutoff)
            .execute(&mut *tx)
            .await?;

        let account_rows_pruned = sqlx::query(
            "DELETE FROM account_snapshot WHERE tenant_id = ? AND substr(as_of, 1, 10) < ?",
        )
        .bind(tenant.0)
        .bind(&raw_cutoff)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        let holding_rows_pruned = sqlx::query(
            "DELETE FROM holding_snapshot WHERE tenant_id = ? AND substr(as_of, 1, 10) < ?",
        )
        .bind(tenant.0)
        .bind(&raw_cutoff)
        .execute(&mut *tx)
        .await?
        .rows_affected();

        let mut aggregate_rows_expired = 0;
        for table in ["account_snapshot_daily", "holding_snapshot_daily"] {
            aggregate_rows_expired += sqlx::query(&format!(
                "DELETE FROM {table} WHERE tenant_id = ? AND day < ?"
            ))
            .bind(tenant.0)
            .bind(&agg_cutoff)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        }
        tx.commit().await?;
        Ok(PruneStats {
            account_rows_pruned,
            holding_rows_pruned,
            aggregate_rows_expired,
        })
    }

    /// Daily account aggregates, ascending by day then account.
    pub async fn account_daily(&self, tenant: TenantId) -> Result<Vec<AccountDailyRow>> {
        sqlx::query(
            "SELECT external_id, day, last_balance_minor, min_balance_minor, \
             max_balance_minor, sample_count FROM account_snapshot_daily \
             WHERE tenant_id = ? ORDER BY day, external_id",
        )
        .bind(tenant.0)
        .fetch_all(self.pool())
        .await?
        .into_iter()
        .map(|r| {
            Ok(AccountDailyRow {
                external_id: r.get("external_id"),
                day: parse_day(r.get("day"), "account_snapshot_daily.day")?,
                last_balance_minor: r.get("last_balance_minor"),
                min_balance_minor: r.get("min_balance_minor"),
                max_balance_minor: r.get("max_balance_minor"),
                sample_count: r.get("sample_count"),
            })
        })
        .collect()
    }

    /// Daily holding aggregates, ascending by day.
    pub async fn holding_daily(&self, tenant: TenantId) -> Result<Vec<HoldingDailyRow>> {
        sqlx::query(
            "SELECT account_external_id, symbol, day, last_market_value_minor, \
             min_market_value_minor, max_market_value_minor, sample_count \
             FROM holding_snapshot_daily WHERE tenant_id = ? \
             ORDER BY day, account_external_id, symbol",
        )
        .bind(tenant.0)
        .fetch_all(self.pool())
        .await?
        .into_iter()
        .map(|r| {
            Ok(HoldingDailyRow {
                account_external_id: r.get("account_external_id"),
                symbol: r.get("symbol"),
                day: parse_day(r.get("day"), "holding_snapshot_daily.day")?,
                last_market_value_minor: r.get("last_market_value_minor"),
                min_market_value_minor: r.get("min_market_value_minor"),
                max_market_value_minor: r.get("max_market_value_minor"),
                sample_count: r.get("sample_count"),
            })
        })
        .collect()
    }
}
