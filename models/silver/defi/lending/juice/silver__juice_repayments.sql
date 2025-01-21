{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = "block_number",
  cluster_by = ['block_timestamp::DATE'],
  tags = ['reorg','curated']
) }}

WITH asset_details AS (

  SELECT
        underlying_asset_address,
        underlying_name,
        underlying_decimals,
        underlying_symbol,
        pool_address,
        token_address,
        token_name,
        token_decimals,
        token_symbol,
        debt_address,
        debt_name,
        debt_decimals,
        debt_symbol
  FROM
    {{ ref('silver__juice_asset_details') }}
),
juice_repayments AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
    CONCAT('0x', SUBSTR(topics[1]:: STRING, 25, 40)) AS borrower,
    contract_address AS token,
    utils.udf_hex_to_int(
      segmented_data [0] :: STRING
    ) :: INTEGER AS repayed_amount_raw,
    'Juice' AS platform,
    modified_timestamp,
    CASE
      WHEN tx_status = 'SUCCESS' THEN TRUE
      ELSE FALSE
    END AS tx_succeeded,
    CONCAT(
      tx_hash :: STRING,
      '-',
      event_index :: STRING
    ) AS _log_id
  FROM
    {{ ref('core__fact_event_logs') }} a
  WHERE
    contract_address IN (
      SELECT
        pool_address
      FROM
        asset_details
    )
    AND topics [0] :: STRING = '0x5c16de4f8b59bd9caf0f49a545f25819a895ed223294290b408242e72a594231'
    AND tx_succeeded
    {% if is_incremental() %}
        AND a.modified_timestamp > (
            SELECT
                max(modified_timestamp)
            FROM
                {{ this }}
        )
        AND a.modified_timestamp >= SYSDATE() - INTERVAL '7 day'
    {% endif %}
    ),
    
juice_combine AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    borrower,
    token,
    C.token_symbol,
    repayed_amount_raw,
    C.underlying_asset_address AS repay_contract_address,
    C.underlying_symbol AS repay_contract_symbol,
    C.underlying_decimals,
    b.platform,
    b._log_id,
    b.modified_timestamp
  FROM
    juice_repayments b
    LEFT JOIN asset_details C
    ON b.token = C.pool_address
)

SELECT
  block_number,
  block_timestamp,
  tx_hash,
  event_index,
  origin_from_address,
  origin_to_address,
  origin_function_signature,
  contract_address,
  contract_address as protocol_market,
  origin_from_address as payer,
  borrower,
  token as token_address,
  token_symbol,
  repay_contract_address,
  repay_contract_symbol,
  repayed_amount_raw AS amount_unadj,
  repayed_amount_raw / pow(
    10,
    underlying_decimals
  ) AS amount,
  platform,
  modified_timestamp,
  _log_id
FROM
  juice_combine qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
  modified_timestamp DESC)) = 1