# Design: Analytics Schema (Data Warehouse)

**Difficulty:** Easy–Medium
**Time:** ~20–30 minutes
**Focus areas:** Star schema, fact/dimension tables, DDL design, analytical query patterns

---

## Problem Statement

Design a business intelligence schema for a stock-trading platform.

Example reports the system must support:
- "Which stocks are being traded by whom, and on which exchanges?"
- "What are the hottest sectors today?"
- "What is the total traded volume per stock per day over the last 30 days?"

The schema should be **general enough** to support many analytics questions while being **optimized for aggregation**.

---

## Background: Analytical vs. Transactional Schemas

| | OLTP (Transactional) | OLAP (Analytical) |
|---|---|---|
| Goal | Fast single-row reads/writes | Fast aggregations over many rows |
| Schema | Highly normalized (3NF) | Denormalized (star/snowflake) |
| Queries | `SELECT * FROM orders WHERE id = 123` | `SELECT SUM(qty) GROUP BY sector, date` |
| Examples | PostgreSQL, MySQL | Hive, Redshift, BigQuery, Snowflake |

**Analytical schemas favor denormalization** to avoid expensive joins at query time. The main pattern is the **Star Schema**.

---

## Star Schema Pattern

```
                  [DIM_STOCK]
                       |
[DIM_DATE] — [FACT_TRADE] — [DIM_TRADER]
                       |
                  [DIM_EXCHANGE]
```

- **Fact table:** The central table. Contains measurable events (trades). Each row = one trade. Contains foreign keys to dimension tables plus metrics.
- **Dimension tables:** Descriptive attributes (stock info, trader info, date attributes, exchange info). Queried via joins in aggregations.

---

## Proposed DDL

### Fact Table: `fact_trade`
```sql
CREATE TABLE fact_trade (
    trade_id        BIGINT       PRIMARY KEY,
    trade_ts        TIMESTAMP    NOT NULL,
    date_key        INT          NOT NULL REFERENCES dim_date(date_key),
    stock_key       INT          NOT NULL REFERENCES dim_stock(stock_key),
    trader_key      INT          NOT NULL REFERENCES dim_trader(trader_key),
    exchange_key    INT          NOT NULL REFERENCES dim_exchange(exchange_key),
    quantity        BIGINT       NOT NULL,   -- shares traded
    price_usd       DECIMAL(18,6) NOT NULL,  -- price per share
    trade_value_usd DECIMAL(18,6) NOT NULL   -- quantity * price (precomputed for performance)
);
```

### Dimension: `dim_stock`
```sql
CREATE TABLE dim_stock (
    stock_key   INT         PRIMARY KEY,
    ticker      VARCHAR(10) NOT NULL,
    company     VARCHAR(255) NOT NULL,
    sector      VARCHAR(100),           -- "Technology", "Energy", etc.
    industry    VARCHAR(100),
    country     VARCHAR(50),
    currency    CHAR(3)                 -- "USD", "EUR"
);
```

### Dimension: `dim_trader`
```sql
CREATE TABLE dim_trader (
    trader_key  INT         PRIMARY KEY,
    trader_id   VARCHAR(50) NOT NULL,
    firm_name   VARCHAR(255),
    trader_type VARCHAR(50),            -- "RETAIL", "INSTITUTIONAL", "INTERNAL"
    region      VARCHAR(50)
);
```

### Dimension: `dim_exchange`
```sql
CREATE TABLE dim_exchange (
    exchange_key    INT          PRIMARY KEY,
    exchange_code   VARCHAR(10)  NOT NULL,  -- "NYSE", "NASDAQ", "LSE"
    exchange_name   VARCHAR(255) NOT NULL,
    country         VARCHAR(50),
    timezone        VARCHAR(50)
);
```

### Dimension: `dim_date`
```sql
CREATE TABLE dim_date (
    date_key    INT     PRIMARY KEY,    -- YYYYMMDD integer for fast joins
    full_date   DATE    NOT NULL,
    year        INT,
    quarter     INT,
    month       INT,
    week        INT,
    day_of_week INT,
    is_holiday  BOOLEAN,
    is_trading_day BOOLEAN
);
```

---

## Example Analytical Queries

### "What are the hottest sectors today?"
```sql
SELECT
    s.sector,
    SUM(t.trade_value_usd) AS total_volume,
    COUNT(*)               AS trade_count
FROM fact_trade t
JOIN dim_stock    s ON t.stock_key    = s.stock_key
JOIN dim_date     d ON t.date_key     = d.date_key
WHERE d.full_date = CURRENT_DATE
GROUP BY s.sector
ORDER BY total_volume DESC
LIMIT 20;
```

### "Which stocks are being traded by whom, and on which exchange?"
```sql
SELECT
    s.ticker,
    s.company,
    tr.firm_name,
    e.exchange_code,
    SUM(t.quantity)        AS total_shares,
    SUM(t.trade_value_usd) AS total_value
FROM fact_trade  t
JOIN dim_stock    s  ON t.stock_key    = s.stock_key
JOIN dim_trader   tr ON t.trader_key   = tr.trader_key
JOIN dim_exchange e  ON t.exchange_key = e.exchange_key
WHERE t.trade_ts >= CURRENT_DATE
GROUP BY s.ticker, s.company, tr.firm_name, e.exchange_code
ORDER BY total_value DESC;
```

### "Total traded volume per stock per day, last 30 days"
```sql
SELECT
    d.full_date,
    s.ticker,
    SUM(t.quantity)        AS total_quantity,
    SUM(t.trade_value_usd) AS total_value
FROM fact_trade t
JOIN dim_stock s ON t.stock_key = s.stock_key
JOIN dim_date  d ON t.date_key  = d.date_key
WHERE d.full_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY d.full_date, s.ticker
ORDER BY d.full_date, total_value DESC;
```

---

## Design Decisions to Discuss

### Why denormalize?
At query time, joining 3–4 small dimension tables to a large fact table is efficient because dimension tables are cached. Fully normalized (3NF) schemas require more joins and worse performance for analytical queries.

### Why `trade_value_usd` as a precomputed column?
`quantity * price` is always the same formula — precompute at write time to avoid recalculating in every query. This is a common optimization in data warehouses.

### Why `date_key` as INT (YYYYMMDD)?
Integer joins are faster than timestamp joins. The dim_date table stores pre-computed date attributes (weekday, holiday, etc.) that are expensive to compute on the fly.

### Partitioning Strategy
In a data warehouse like Hive, Redshift, or BigQuery:
- Partition `fact_trade` by `date_key` — nearly all queries filter by date.
- Cluster/sort by `stock_key` — many queries also filter by ticker.

---

## What to Cover in Your Answer

- [ ] Star schema structure: one fact table + multiple dimension tables
- [ ] What goes in the fact table (measurable events, metrics, foreign keys)
- [ ] What goes in dimension tables (descriptive attributes)
- [ ] At least 3–4 column-level decisions with reasoning (nullable, precomputed, data types)
- [ ] Example query showing how the schema supports the required reports
- [ ] Partitioning strategy for query performance

---

## Follow-Up Questions

1. "How would you extend this schema to support options trading (not just equities)?"
2. "If `dim_stock.sector` changes (e.g., a company moves from Tech to Healthcare), how do you handle that historically?"  *(Answer: Slowly Changing Dimensions — SCD Type 2)*
3. "At 1 billion trades per day, what changes to your fact table design?"
4. "How is this schema different from a 3NF transactional schema? When would you use each?"
5. "You want to add a real-time trading dashboard. Does this star schema support that, or do you need something else?"
