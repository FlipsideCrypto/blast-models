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
    {{ ref('silver__init_asset_details') }}
),
init_borrows AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_from_address AS borrower,
    origin_function_signature,
    contract_address,
    topics,
    regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
    utils.udf_hex_to_int(
      topics [2] :: STRING
    ) :: FLOAT AS posId,
    CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS pool,
    utils.udf_hex_to_int(
      segmented_data [0] :: STRING
    ) :: FLOAT AS loan_amount_raw,
    -- receipt token
    utils.udf_hex_to_int(
      segmented_data [1] :: STRING
    ) :: FLOAT AS sharesAmt,
    -- receipt token
    contract_address AS token,
    'INIT Capital' AS platform,
    modified_timestamp,
    tx_succeeded,
    CONCAT(
      tx_hash :: STRING,
      '-',
      event_index :: STRING
    ) AS _log_id
  FROM
    {{ ref('core__fact_event_logs') }}
  WHERE
    contract_address = '0xa7d36f2106b5a5d528a7e2e7a3f436d703113a10'
    AND topics [0] :: STRING = '0x49dd87b26edb1c92c93f83b092bd5a425c6bf7a562c0fed02f2576c49f477ba4'
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
token_transfer AS (
  SELECT
    tx_hash,
    contract_address,
    from_address,
    to_address,
    raw_amount AS underlying_amount_raw,
    token_decimals,
    token_symbol,
    token_name
  FROM
    {{ ref('core__fact_token_transfers') }}
    LEFT JOIN {{ ref('silver__contracts') }} USING(contract_address)
  WHERE
    contract_address IN (
      '0xb1a5700fa2358173fe465e6ea4ff52e36e88e2ad',
      '0x4300000000000000000000000000000000000003',
      '0x4300000000000000000000000000000000000004'
    )
    AND (
      from_address IN (
        SELECT
          token_address
        FROM
          asset_details
      ) -- for Blast
      OR from_address IN (
        SELECT
          underlying_asset_address
        FROM
          asset_details
      )
    ) -- to get USDB from WUSDB or WETH FROM WWETH
    AND tx_hash IN (
      SELECT
        tx_hash
      FROM
        init_borrows
    )
),
native_transfer AS (
  SELECT
    tx_hash,
    from_address AS wrapped_address,
    to_address,
    value_precise_raw AS eth_value,
    'WETH' AS eth_symbol,
    trace_succeeded,
    18 AS eth_decimals,
    '0x4300000000000000000000000000000000000004' AS eth_address
  FROM
    {{ ref('core__fact_traces') }}
  WHERE
    from_address IN ('0xf683ce59521aa464066783d78e40cd9412f33d21')
    AND tx_hash IN (
      SELECT
        tx_hash
      FROM
        init_borrows
    )
    AND TYPE = 'CALL'
    AND trace_succeeded
    AND input = '0x'
),
init_combine AS (
  SELECT
    block_number,
    block_timestamp,
    b.tx_hash,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    b.contract_address,
    borrower,
    pool,
    loan_amount_raw,
    C.underlying_decimals AS underlying_wrap_decimals,
    COALESCE(
      eth_value,
      underlying_amount_raw
    ) AS underlying_amount_raw,
    COALESCE(
      eth_symbol,
      d.token_symbol,
      C.underlying_symbol
    ) AS underlying_symbol,
    COALESCE(
      eth_decimals,
      d.token_decimals,
      C.underlying_decimals
    ) AS underlying_decimals,
    COALESCE(
      eth_address,
      C.underlying_asset_address
    ) AS underlying_asset_address,
    underlying_asset_address AS borrows_contract_address,
    underlying_symbol AS borrows_symbol,
    b.contract_address AS token,
    C.token_symbol,
    C.token_decimals,
    b.platform,
    b._log_id,
    b.modified_timestamp
  FROM
    init_borrows b
    LEFT JOIN asset_details C
    ON b.pool = C.token_address
    LEFT JOIN token_transfer d
    ON b.tx_hash = d.tx_hash
    LEFT JOIN native_transfer e
    ON b.tx_hash = e.tx_hash
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
  underlying_symbol,
  loan_amount_raw AS amount_unadj,
  loan_amount_raw / pow(
    10,
    underlying_wrap_decimals
  ) AS amount,
  underlying_amount_raw,
  underlying_amount_raw / pow(
    10,
    underlying_decimals
  ) AS underlyingAmount,
  platform,
  modified_timestamp,
  _log_id
FROM
  init_combine qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
  modified_timestamp DESC)) = 1
