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
juice_deposits AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    contract_address AS token_address,
    regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
    utils.udf_hex_to_int(
      segmented_data [2] :: STRING
    ) :: INTEGER AS minttokens_raw,
    utils.udf_hex_to_int(
      segmented_data [1] :: STRING
    ) :: INTEGER AS mintAmount_raw,
    CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS supplier,
    'Orbit' AS platform,
    modified_timestamp,
    _log_id
  FROM
    {{ ref('core__fact_event_logs') }} a
  WHERE
    contract_address IN (
      SELECT
        token_address
      FROM
        asset_details
    )
    AND topics [0] :: STRING = '0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f'
    AND tx_status = 'SUCCESS'
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
    b.block_number,
    b.block_timestamp,
    b.tx_hash,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    supplier,
    minttokens_raw,
    mintAmount_raw,
    C.underlying_asset_address AS supplied_contract_address,
    C.underlying_symbol AS supplied_symbol,
    C.token_address,
    C.token_symbol,
    C.token_decimals,
    C.underlying_decimals,
    b.platform,
    b._log_id,
    b.modified_timestamp
  FROM
    juice_deposits b
    LEFT JOIN asset_details C
    ON b.token_address = C.token_address
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
  token_address,
  token_symbol,
  minttokens_raw / pow(
    10,
    token_decimals
  ) AS issued_tokens,
  mintAmount_raw AS amount_unadj,
  mintAmount_raw / pow(
    10,
    underlying_decimals
  ) AS amount,
  supplied_contract_address,
  supplied_symbol,
  supplier,
  platform,
  modified_timestamp,
  _log_id
FROM
  juice_combine qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
  modified_timestamp DESC)) = 1