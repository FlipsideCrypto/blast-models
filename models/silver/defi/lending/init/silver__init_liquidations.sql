{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = "block_number",
  cluster_by = ['block_timestamp::DATE'],
  tags = ['reorg','curated']
) }}

WITH asset_details AS (

  SELECT
    block_timestamp,
    block_number,
    tx_hash,
    token_address,
    token_name,
    token_symbol,
    token_decimals,
    underlying_asset_address,
    underlying_name,
    underlying_symbol,
    underlying_decimals,
    underlying_unwrap_address,
    underlying_unwrap_name,
    underlying_unwrap_symbol,
    underlying_unwrap_decimals,
    _inserted_timestamp
  FROM
    {{ ref('silver__init_asset_details') }}
),
init_liquidations AS (
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
      topics [1] :: STRING
    ) :: FLOAT AS posId,
    CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS liquidator,
    CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS poolOut,
    utils.udf_hex_to_int(
      segmented_data [1] :: STRING
    ) :: FLOAT AS sharesamt,
    -- receipt token
    contract_address AS token,
    'INIT Capital' AS platform,
    inserted_timestamp AS _inserted_timestamp,
    _log_id
  FROM
    {{ ref('core__fact_event_logs') }}
  WHERE
    contract_address = '0xa7d36f2106b5a5d528a7e2e7a3f436d703113a10'
    AND topics [0] :: STRING = '0x6df71caf4cddb1620bcf376243248e0077da98913d65a7e9315bc9984e5fff72'
    AND tx_status = 'SUCCESS'
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
    1 = 1
    AND (
      contract_address IN (
        '0xb1a5700fa2358173fe465e6ea4ff52e36e88e2ad',
        '0x4300000000000000000000000000000000000003',
        '0x4300000000000000000000000000000000000004'
      )
      OR contract_address IN (
        SELECT
          underlying_asset_address
        FROM
          asset_details
      )
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
    ) -- to get USDB from WUSDB
    AND tx_hash IN (
      SELECT
        tx_hash
      FROM
        init_liquidations
    )
)
SELECT
  l.block_number,
  l.block_timestamp,
  l.tx_hash,
  event_index,
  origin_from_address,
  origin_to_address,
  origin_function_signature,
  l.contract_address,
  borrower,
  token,
  C.token_symbol,
  poolOut,
  token AS protocol_market,
  l.sharesamt AS amount_unadj,
  underlying_amount_raw,
  l.sharesamt / pow(
    10,
    C.token_decimals
  ) AS tokens_seized,
  -- in tokens
  underlying_amount_raw / pow(
    10,
    d.token_decimals
  ) AS amount,
  C.underlying_decimals,
  C.underlying_asset_address AS liquidation_contract_address,
  C.underlying_symbol AS liquidation_contract_symbol,
  l.platform,
  l._inserted_timestamp,
  l._log_id
FROM
  init_liquidations l
  LEFT JOIN asset_details C
  ON l.poolOut = C.token_address
  LEFT JOIN token_transfer d
  ON l.tx_hash = d.tx_hash
  AND C.underlying_asset_address = d.from_address
