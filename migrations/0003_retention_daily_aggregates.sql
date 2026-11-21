-- Retention (additive; 0001 is untouched and stays frozen).
-- NOTE: 0001's "append-only / never DELETE" comments on the snapshot tables
-- are superseded here: only `veille prune` may delete from them.
-- `veille prune` aggregates raw snapshot rows older than the raw window into
-- one row per entity per UTC day, then deletes the raw rows, in one
-- transaction. Aggregates themselves expire after the aggregate window.
-- The UNIQUE keys make pruning idempotent: a re-run (or a late row for an
-- already-aggregated day) merges into the existing row.

CREATE TABLE account_snapshot_daily (
    id                 INTEGER PRIMARY KEY,
    tenant_id          INTEGER NOT NULL REFERENCES tenant(id),
    external_id        TEXT NOT NULL,
    day                TEXT NOT NULL,  -- YYYY-MM-DD (UTC date of as_of)
    name               TEXT NOT NULL,  -- as of the last observation of the day
    kind               TEXT NOT NULL,
    currency           TEXT NOT NULL,
    last_balance_minor INTEGER NOT NULL,
    min_balance_minor  INTEGER NOT NULL,
    max_balance_minor  INTEGER NOT NULL,
    last_as_of         TEXT NOT NULL,  -- RFC 3339 UTC of the last observation
    sample_count       INTEGER NOT NULL,
    UNIQUE (tenant_id, external_id, day)
);

CREATE TABLE holding_snapshot_daily (
    id                      INTEGER PRIMARY KEY,
    tenant_id               INTEGER NOT NULL REFERENCES tenant(id),
    account_external_id     TEXT NOT NULL,
    symbol                  TEXT NOT NULL,
    day                     TEXT NOT NULL,
    quantity                TEXT NOT NULL,  -- last observation of the day
    currency                TEXT NOT NULL,
    position_date           TEXT NOT NULL,
    last_market_value_minor INTEGER,        -- NULL when the last observation was unparseable
    min_market_value_minor  INTEGER,        -- min/max ignore NULL observations
    max_market_value_minor  INTEGER,
    last_as_of              TEXT NOT NULL,
    sample_count            INTEGER NOT NULL,
    UNIQUE (tenant_id, account_external_id, symbol, day)
);
