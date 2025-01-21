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
juice_borrows AS (
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
    CONCAT('0x', SUBSTR(topics [1] :: STRING, 25, 40)) AS borrower,
    utils.udf_hex_to_int (
      segmented_data [0]
    ) AS loan_amount_raw,
    contract_address AS pool_address,
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
    {{ ref('core__fact_event_logs') }} A
    LEFT JOIN asset_details
    ON contract_address = pool_address
  WHERE
    contract_address IN (
      SELECT
        pool_address
      FROM
        asset_details
    )
    AND topics [0] :: STRING = '0xcbc04eca7e9da35cb1393a6135a199ca52e450d5e9251cbd99f7847d33a36750'
    AND tx_succeeded

{% if is_incremental() %}
AND A.modified_timestamp > (
  SELECT
    MAX(modified_timestamp)
  FROM
    {{ this }}
)
AND A.modified_timestamp >= SYSDATE() - INTERVAL '7 day'
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
    loan_amount_raw,
    C.underlying_asset_address AS borrows_contract_address,
    C.underlying_symbol AS borrows_symbol,
    contract_address AS token,
    C.token_symbol,
    C.underlying_decimals,
    debt_name,
    debt_address,
    debt_symbol,
    debt_decimals,
    b.platform,
    b._log_id,
    b.modified_timestamp
  FROM
    juice_borrows b
    LEFT JOIN asset_details C
    ON b.contract_address = C.pool_address
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
  borrower,
  borrows_contract_address,
  borrows_symbol,
  debt_name AS token_name,
  debt_address AS token_address,
  debt_symbol AS token_symbol,
  loan_amount_raw AS amount_unadj,
  loan_amount_raw / pow(
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
