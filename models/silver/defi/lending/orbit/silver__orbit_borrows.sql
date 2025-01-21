{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = "block_number",
  cluster_by = ['block_timestamp::DATE'],
  tags = ['reorg','curated']
) }}

WITH asset_details AS (

  SELECT
    token_address,
    token_name,
    token_symbol,
    token_decimals,
    underlying_asset_address,
    underlying_name,
    underlying_symbol,
    underlying_decimals
  FROM
    {{ ref('silver__orbit_asset_details') }}
),
orbit_borrows AS (
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
    CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS borrower,
    utils.udf_hex_to_int(
      segmented_data [1] :: STRING
    ) :: INTEGER AS loan_amount_raw,
    utils.udf_hex_to_int(
      segmented_data [2] :: STRING
    ) :: INTEGER AS accountBorrows,
    utils.udf_hex_to_int(
      segmented_data [3] :: STRING
    ) :: INTEGER AS totalBorrows,
    contract_address AS token,
    'Orbit' AS platform,
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
    {{ ref('core__fact_event_logs') }}
  WHERE
    contract_address IN (
      SELECT
        token_address
      FROM
        asset_details
    )
    AND topics [0] :: STRING = '0x13ed6866d4e1ee6da46f845c46d7e54120883d75c5ea9a2dacc1c4ca8984ab80'
    AND tx_succeeded

{% if is_incremental() %}
AND modified_timestamp > (
  SELECT
    MAX(modified_timestamp)
  FROM
    {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
orbit_combine AS (
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
    token,
    C.token_symbol,
    C.underlying_decimals,
    b.platform,
    b._log_id,
    b.modified_timestamp
  FROM
    orbit_borrows b
    LEFT JOIN asset_details C
    ON b.token = C.token_address
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
  token AS token_address,
  token_symbol,
  loan_amount_raw AS amount_unadj,
  loan_amount_raw / pow(
    10,
    underlying_decimals
  ) AS amount,
  platform,
  modified_timestamp,
  _log_id
FROM
  orbit_combine qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
  modified_timestamp DESC)) = 1
