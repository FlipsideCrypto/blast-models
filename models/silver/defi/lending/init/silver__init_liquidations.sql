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
    modified_timestamp
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
    contract_address AS token,
    'INIT Capital' AS platform,
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
    contract_address = '0xa7d36f2106b5a5d528a7e2e7a3f436d703113a10'
    AND topics [0] :: STRING = '0x6df71caf4cddb1620bcf376243248e0077da98913d65a7e9315bc9984e5fff72'
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
liquidation_union AS (
  SELECT
    l.block_number,
    l.block_timestamp,
    l.tx_hash,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    l.contract_address,
    posId,
    C.token_symbol AS collateral_token_symbol,
    poolOut AS collateral_token,
    liquidator,
    l.sharesamt AS tokens_seized_raw,
    l.sharesamt / pow(
      10,
      C.token_decimals
    ) AS tokens_seized,
    -- in tokens
    C.underlying_decimals,
    l.platform,
    l.modified_timestamp,
    l._log_id
  FROM
    init_liquidations l
    LEFT JOIN asset_details C
    ON l.poolOut = C.token_address
),
init_repayment AS (
  SELECT
    tx_hash,
    protocol_market,
    token_address AS debt_token,
    token_symbol AS debt_token_symbol,
    posId,
    amount_unadj,
    amount
  FROM
    {{ ref('silver__init_repayments') }}
  WHERE
    tx_hash IN (
      SELECT
        tx_hash
      FROM
        init_liquidations
    )
),
position_owner AS (
  SELECT
    regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
    CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS borrower,
    utils.udf_hex_to_int(
      topics [2] :: STRING
    ) :: STRING AS posId, -- using string as it handles better than float
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
    contract_address = '0xa7d36f2106b5a5d528a7e2e7a3f436d703113a10'
    AND topics [0] :: STRING = '0xe6a96441ecc85d0943a914f4750f067a912798ec2543bc68c00e18291da88d14' -- createposition
    AND tx_succeeded
    AND posId IN (
      SELECT
        posId
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
  l.contract_address AS token,
  liquidator,
  borrower,
  protocol_market,
  collateral_token,
  collateral_token_symbol,
  tokens_seized_raw AS amount_unadj,
  tokens_seized AS amount,
  debt_token,
  debt_token_symbol,
  platform,
  modified_timestamp,
  l._log_id
FROM
  liquidation_union l
  LEFT JOIN position_owner po
  ON l.posId = po.posId
  LEFT JOIN init_repayment r
  ON l.posId = r.posId
  AND l.tx_hash = r.tx_hash qualify(ROW_NUMBER() over(PARTITION BY l._log_id
ORDER BY
  modified_timestamp DESC)) = 1
