/* =====================================================================
   STAGING VIEW: STG_SUPERSTORE
   - Cleans raw Superstore data
   - Safely parses order/ship dates (works even if raw is DATE/TIMESTAMP)
   - Adds date attributes for Power BI
   ===================================================================== */

CREATE OR REPLACE VIEW SUPERSTORE_DB.STAGING.STG_SUPERSTORE AS

/* ---------------------------------------------------------------------
   Base source (raw)
   --------------------------------------------------------------------- */
WITH src AS (
  SELECT
    ROW_ID,
    ORDER_ID,
    ORDER_DATE,
    SHIP_DATE,
    SHIP_MODE,
    CUSTOMER_ID,
    CUSTOMER_NAME,
    SEGMENT,
    COUNTRY,
    CITY,
    STATE,
    POSTAL_CODE,
    REGION,
    PRODUCT_ID,
    CATEGORY,
    "Sub-Category",
    PRODUCT_NAME,
    SALES,
    QUANTITY,
    DISCOUNT,
    PROFIT,
    REGIONAL_MANAGER,
    TARGET_PROFIT_MARGIN,
    PROFIT_MARGIN,
    ABOVE_TARGET,
    DAYS_TO_SHIP,
    SHIP_PERFORMANCE
  FROM SUPERSTORE_DB.RAW.SUPERSTORE_RAW
  WHERE ORDER_ID IS NOT NULL
),

/* ---------------------------------------------------------------------
   Date parsing (Snowflake-safe)
   - Cast raw values to VARCHAR first to avoid DATE/TIMESTAMP type conflicts
   --------------------------------------------------------------------- */
typed AS (
  SELECT
    COALESCE(
      TO_DATE(TRY_TO_TIMESTAMP(ORDER_DATE::VARCHAR, 'MM/DD/YYYY')),
      TO_DATE(TRY_TO_TIMESTAMP(ORDER_DATE::VARCHAR, 'M/D/YYYY')),
      TO_DATE(TRY_TO_TIMESTAMP(ORDER_DATE::VARCHAR, 'YYYY-MM-DD')),
      TRY_TO_DATE(ORDER_DATE::VARCHAR)
    ) AS order_date_parsed,

    COALESCE(
      TO_DATE(TRY_TO_TIMESTAMP(SHIP_DATE::VARCHAR, 'MM/DD/YYYY')),
      TO_DATE(TRY_TO_TIMESTAMP(SHIP_DATE::VARCHAR, 'M/D/YYYY')),
      TO_DATE(TRY_TO_TIMESTAMP(SHIP_DATE::VARCHAR, 'YYYY-MM-DD')),
      TRY_TO_DATE(SHIP_DATE::VARCHAR)
    ) AS ship_date_parsed,

    s.*
  FROM src s
)

/* ---------------------------------------------------------------------
   Final select (analysis-ready)
   --------------------------------------------------------------------- */
SELECT
  /* --- Identifiers --- */
  ROW_ID::NUMBER                         AS row_id,
  ORDER_ID::VARCHAR                      AS order_id,

  /* --- Dates --- */
  order_date_parsed                      AS order_date,
  ship_date_parsed                       AS ship_date,

  /* --- Order date attributes (Power BI friendly) --- */
  YEAR(order_date_parsed)                AS order_year,
  MONTH(order_date_parsed)               AS order_month,
  TO_VARCHAR(order_date_parsed, 'MON')   AS order_month_name,
  QUARTER(order_date_parsed)             AS order_quarter,
  TO_VARCHAR(order_date_parsed, 'YYYY-MM')            AS order_year_month,
  (YEAR(order_date_parsed) * 100 + MONTH(order_date_parsed))::NUMBER
                                         AS order_year_month_key,
  DATE_TRUNC('MONTH', order_date_parsed)::DATE        AS order_month_start,

  /* --- Shipping date attributes (optional) --- */
  YEAR(ship_date_parsed)                 AS ship_year,
  MONTH(ship_date_parsed)                AS ship_month,
  TO_VARCHAR(ship_date_parsed, 'YYYY-MM')            AS ship_year_month,
  (YEAR(ship_date_parsed) * 100 + MONTH(ship_date_parsed))::NUMBER
                                         AS ship_year_month_key,

  /* --- Shipping --- */
  SHIP_MODE::VARCHAR                     AS ship_mode,

  /* --- Customer --- */
  CUSTOMER_ID::VARCHAR                   AS customer_id,
  CUSTOMER_NAME::VARCHAR                 AS customer_name,
  SEGMENT::VARCHAR                       AS segment,

  /* --- Geography --- */
  COUNTRY::VARCHAR                       AS country,
  CITY::VARCHAR                          AS city,
  STATE::VARCHAR                         AS state,
  POSTAL_CODE::VARCHAR                   AS postal_code,
  REGION::VARCHAR                        AS region,

  /* --- Region display (extra column for Power BI matrix) --- */
  REGION || ' (' || REGIONAL_MANAGER || ')'           AS region_display,

  /* --- Product --- */
  PRODUCT_ID::VARCHAR                    AS product_id,
  CATEGORY::VARCHAR                      AS category,
  "Sub-Category"::VARCHAR                AS sub_category,
  PRODUCT_NAME::VARCHAR                  AS product_name,

  /* --- Metrics --- */
  SALES::NUMBER(18,4)                    AS sales,
  QUANTITY::NUMBER(18,0)                 AS quantity,
  DISCOUNT::NUMBER(18,4)                 AS discount,
  PROFIT::NUMBER(18,4)                   AS profit,

  /* --- Targets & performance --- */
  REGIONAL_MANAGER::VARCHAR              AS regional_manager,
  TARGET_PROFIT_MARGIN::NUMBER(18,4)     AS target_profit_margin,
  PROFIT_MARGIN::NUMBER(18,4)            AS profit_margin,

  CASE
    WHEN UPPER(TRIM(ABOVE_TARGET)) IN ('YES','Y','TRUE','1') THEN 'Yes'
    WHEN UPPER(TRIM(ABOVE_TARGET)) IN ('NO','N','FALSE','0') THEN 'No'
    ELSE NULL
  END                                    AS above_target,

  /* --- Operations --- */
  DAYS_TO_SHIP::NUMBER(18,0)             AS days_to_ship,
  SHIP_PERFORMANCE::VARCHAR              AS ship_performance

FROM typed;
