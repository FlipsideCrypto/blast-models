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
    contract_address,
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
juice_liquidations AS (
  SELECT
    l.block_number,
    l.block_timestamp,
    l.tx_hash,
    l.event_index,
    l.origin_from_address,
    l.origin_to_address,
    l.origin_function_signature,
    l.contract_address,
    regexp_substr_all(SUBSTR(l.data, 3, len(l.data)), '.{64}') AS segmented_data,
    CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS borrower,
    l.origin_to_address as liquidator,
    l.contract_address AS token,
    utils.udf_hex_to_int(
      segmented_data [1] :: STRING
    ) :: INTEGER AS repayAmount_raw,
    utils.udf_hex_to_int(
      segmented_data [2] :: STRING
    ) :: INTEGER AS bonuscollateral_raw,
    utils.udf_hex_to_int(
      segmented_data [3] :: STRING
    ) :: INTEGER AS debtamountneeded_raw,
    'Juice' AS platform,
    l.modified_timestamp,
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
    l
    INNER JOIN asset_details cl
    ON l.contract_address = LOWER(
      cl.contract_address
    )
  WHERE
    l.topics [0] :: STRING = '0xe32ec3ea3154879f27d5367898ab3a5ac6b68bf921d7cc610720f417c5cb243c'
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
token_transfer AS (
  SELECT
    block_timestamp,
    tx_hash,
    utils.udf_hex_to_int(DATA) AS seizeTokens_raw,
    event_index,
    CASE
      WHEN tx_status = 'SUCCESS' THEN TRUE
      ELSE FALSE
    END AS tx_succeeded,
    CONCAT(
      tx_hash :: STRING,
      '-',
      event_index :: STRING
    ) AS _log_id,
    ROW_NUMBER() over (
      PARTITION BY _log_id
      ORDER BY
        event_index ASC
    )
  FROM
    {{ ref('core__fact_event_logs') }}
  WHERE
    contract_address IN (
      SELECT
        token_address
      FROM
        asset_details
    )
    AND topics [2] = '0x0000000000000000000000000000000000000000000000000000000000000000'
    AND topics [0] = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    AND tx_hash IN (
      SELECT
        tx_hash
      FROM
        juice_liquidations
    )
    AND tx_succeeded qualify(ROW_NUMBER() over(PARTITION BY tx_hash
  ORDER BY
    event_index ASC)) = 1
),
debt_transfer AS (
  SELECT
    block_timestamp,
    tx_hash,
    utils.udf_hex_to_int(DATA) AS debtamount,
    debt_name,
    debt_address AS debt_token,
    debt_symbol AS debt_token_symbol,
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
    l
    INNER JOIN asset_details
    ON debt_address = l.contract_address
  WHERE
    topics [2] = '0x0000000000000000000000000000000000000000000000000000000000000000'
    AND topics [0] = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    AND tx_hash IN (
      SELECT
        tx_hash
      FROM
        juice_liquidations
    )
    AND tx_succeeded
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
      asd1.token_decimals
    ) AS tokens_seized,
    token AS protocol_market,
    repayAmount_raw AS amount_unadj,
    repayAmount_raw / pow(
      10,
      asd1.underlying_decimals
    ) AS amount,
    asd1.underlying_decimals,
    asd1.underlying_asset_address AS collateral_token,
    asd1.underlying_symbol AS collateral_token_symbol,
    debt_token,
    debt_token_symbol,
    l.platform,
    l.modified_timestamp,
    l._log_id
  FROM
    juice_liquidations l
    LEFT JOIN asset_details asd1
    ON l.token = asd1.token_address
    LEFT JOIN token_transfer USING(tx_hash)
    LEFT JOIN debt_transfer USING(tx_hash)
)
SELECT
  *
FROM
  liquidation_union qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
  modified_timestamp DESC)) = 1
