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
orbit_liquidations AS (
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
    CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS borrower,
    contract_address AS token,
    CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS liquidator,
    utils.udf_hex_to_int(
      segmented_data [4] :: STRING
    ) :: INTEGER AS seizeTokens_raw,
    utils.udf_hex_to_int(
      segmented_data [2] :: STRING
    ) :: INTEGER AS repayAmount_raw,
    CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 25, 40)) AS tokenCollateral,
    'Orbit' AS platform,
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
    contract_address IN (
      SELECT
        token_address
      FROM
        asset_details
    )
    AND topics [0] :: STRING = '0x298637f684da70674f26509b10f07ec2fbc77a335ab1e7d6215a4b2484d8bb52'
    AND tx_succeeded

{% if is_incremental() %}
AND modified_timestamp >= (
  SELECT
    MAX(modified_timestamp) - INTERVAL '12 hours'
  FROM
    {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
liquidation_union AS (
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
    asd1.token_symbol AS token_symbol,
    liquidator,
    seizeTokens_raw,
    seizeTokens_raw / pow(
      10,
      asd2.token_decimals
    ) AS tokens_seized,
    tokenCollateral AS protocol_market,
    asd2.underlying_asset_address AS collateral_token,
    asd2.underlying_symbol AS collateral_token_symbol,
    repayAmount_raw AS amount_unadj,
    repayAmount_raw / pow(
      10,
      asd1.underlying_decimals
    ) AS amount,
    asd1.underlying_decimals,
    asd1.underlying_asset_address AS liquidation_contract_address,
    asd1.underlying_symbol AS liquidation_contract_symbol,
    l.platform,
    l.modified_timestamp,
    l._log_id
  FROM
    orbit_liquidations l
    LEFT JOIN asset_details asd1
    ON l.token = asd1.token_address
    LEFT JOIN asset_details asd2
    ON l.tokenCollateral = asd2.token_address
)
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  contract_address,
  'LiquidateBorrow' AS event_name,
  event_index,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  liquidator,
  borrower,
  token,
  token_symbol,
  contract_address AS protocol_market,
  collateral_token,
  collateral_token_symbol,
  amount_unadj,
  amount,
  liquidation_contract_address AS debt_token,
  liquidation_contract_symbol AS debt_token_symbol,
  platform,
  modified_timestamp,
  _log_id
FROM
  liquidation_union qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
  modified_timestamp DESC)) = 1
