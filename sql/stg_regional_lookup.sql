CREATE OR REPLACE VIEW SUPERSTORE_DB.STAGING.stg_regional_lookup AS
SELECT
  REGION::VARCHAR           AS region,
  REGIONAL_MANAGER::VARCHAR AS regional_manager,

  CASE
    WHEN TARGET_PROFIT_MARGIN IS NULL THEN NULL

    /* If value contains a % sign → treat as percent */
    WHEN REGEXP_LIKE(TARGET_PROFIT_MARGIN::VARCHAR, '%') THEN
      TRY_TO_NUMBER(
        REGEXP_REPLACE(TARGET_PROFIT_MARGIN::VARCHAR, '[^0-9\.\-]', '')
      ) / 100

    /* If value looks like an integer (14) → assume percent */
    WHEN TRY_TO_NUMBER(
      REGEXP_REPLACE(TARGET_PROFIT_MARGIN::VARCHAR, '[^0-9\.\-]', '')
    ) >= 1 THEN
      TRY_TO_NUMBER(
        REGEXP_REPLACE(TARGET_PROFIT_MARGIN::VARCHAR, '[^0-9\.\-]', '')
      ) / 100

    /* Already a decimal (0.14) */
    ELSE
      TRY_TO_NUMBER(
        REGEXP_REPLACE(TARGET_PROFIT_MARGIN::VARCHAR, '[^0-9\.\-]', '')
      )
  END::NUMBER(10,4) AS target_profit_margin

FROM SUPERSTORE_DB.RAW.REGIONAL_LOOKUP_RAW;

