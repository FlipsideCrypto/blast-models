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
deposit_logs AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    event_index,
    contract_address,
    topics,
    DATA,
    event_removed,
    tx_status,
    _log_id,
    fact_event_logs_id,
    inserted_timestamp,
    modified_timestamp
  FROM
    {{ ref('core__fact_event_logs') }}
  WHERE
    (
      contract_address IN (
        SELECT
          pool_address
        FROM
          asset_details
      )
      OR contract_address IN (
        SELECT
          token_address
        FROM
          asset_details
      )
    )
    AND topics [0] :: STRING IN (
      '0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c',
      '0xd88c5369d398bea6a7390a17ce98af43f4aacc78fd3587bc368993d98206a304',
      '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    )
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
    regexp_substr_all(SUBSTR(DATA :: STRING, 3, len(DATA)), '.{64}') AS segmented_data,
    TRY_CAST(
      utils.udf_hex_to_int(
        segmented_data [0] :: STRING
      ) AS DECIMAL(
        38,
        0
      )
    ) AS mintAmount_raw,
    CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS supplier,
    'Juice' AS platform,
    inserted_timestamp AS modified_timestamp,
    _log_id
  FROM
    deposit_logs
  WHERE
    contract_address IN (
      SELECT
        pool_address
      FROM
        asset_details
    )
    AND topics [0] :: STRING = '0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c'
    AND tx_status = 'SUCCESS'
),
juice_collateraldeposits AS (
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
    regexp_substr_all(SUBSTR(DATA :: STRING, 3, len(DATA)), '.{64}') AS segmented_data,
    utils.udf_hex_to_int(
      segmented_data [2] :: STRING
    ) :: INTEGER AS mintAmount_raw,
    CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS supplier,
    'Juice' AS platform,
    inserted_timestamp AS modified_timestamp,
    _log_id
  FROM
    deposit_logs
  WHERE
    contract_address IN (
      SELECT
        token_address
      FROM
        asset_details
    )
    AND topics [0] :: STRING = '0xd88c5369d398bea6a7390a17ce98af43f4aacc78fd3587bc368993d98206a304'
    AND tx_status = 'SUCCESS'
),
token_transfer AS (
  SELECT
    block_timestamp,
    tx_hash,
    utils.udf_hex_to_int(DATA) AS minttokens_raw
  FROM
    deposit_logs
  WHERE
    1 = 1
    AND contract_address IN (
      SELECT
        token_address
      FROM
        asset_details
    )
    AND topics [1] :: STRING = '0x0000000000000000000000000000000000000000000000000000000000000000'
    AND topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    AND tx_hash IN (
      SELECT
        tx_hash
      FROM
        juice_collateraldeposits
      UNION ALL
      SELECT
        tx_hash
      FROM
        juice_deposits
    )
    AND tx_status = 'SUCCESS'
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
    LEFT JOIN token_transfer d USING(tx_hash)
    LEFT JOIN asset_details C
    ON b.token_address = C.pool_address
  UNION ALL
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
    juice_collateraldeposits b
    LEFT JOIN token_transfer d USING(tx_hash)
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
