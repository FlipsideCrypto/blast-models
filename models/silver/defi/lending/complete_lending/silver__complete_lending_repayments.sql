-- depends_on: {{ ref('silver__complete_token_prices') }}
{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['block_number','platform'],
  cluster_by = ['block_timestamp::DATE','platform'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, origin_from_address, origin_to_address, origin_function_signature, contract_address, event_name, token_address, token_symbol, payer, borrower, protocol_market), SUBSTRING(origin_function_signature, event_name, token_address, token_symbol, payer, borrower, protocol_market)",
  tags = ['reorg','curated','heal']
) }}

WITH init AS (
  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    repay_contract_address AS token_address,
    token_address AS protocol_market,
    amount_unadj,
    amount,
    repay_contract_symbol AS token_symbol,
    origin_from_address AS payer_address,
    borrower,
    platform,
    'blast' AS blockchain,
    _LOG_ID,
    modified_timestamp as _inserted_timestamp
  FROM
    {{ ref('silver__init_repayments') }}

{% if is_incremental() and 'init' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
juice AS (
  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    repay_contract_address AS token_address,
    token_address AS protocol_market,
    amount_unadj,
    amount,
    repay_contract_symbol AS token_symbol,
    origin_from_address AS payer_address,
    borrower,
    platform,
    'blast' AS blockchain,
    _LOG_ID,
    modified_timestamp as _inserted_timestamp
  FROM
    {{ ref('silver__juice_repayments') }}

{% if is_incremental() and 'juice' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
orbit AS (
  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    repay_contract_address AS token_address,
    token_address AS protocol_market,
    amount_unadj,
    amount,
    repay_contract_symbol AS token_symbol,
    origin_from_address AS payer_address,
    borrower,
    platform,
    'blast' AS blockchain,
    _LOG_ID,
    modified_timestamp as _inserted_timestamp
  FROM
    {{ ref('silver__orbit_repayments') }}

{% if is_incremental() and 'orbit' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),

repayments_union AS (
  SELECT
    *
  FROM
    init
  UNION ALL
  SELECT
    *
  FROM
    juice
  UNION ALL
  SELECT
    *
  FROM
    orbit
),
complete_lending_repayments AS (
  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    A.contract_address,
    CASE
      WHEN platform IN (
        'Sonne',
        'Moonwell'
      ) THEN 'RepayBorrow'
      WHEN platform = 'Compound V3' THEN 'Supply'
      ELSE 'Repay'
    END AS event_name,
    protocol_market,
    payer_address AS payer,
    borrower,
    A.token_address,
    A.token_symbol,
    amount_unadj,
    amount,
    ROUND(
      amount * price,
      2
    ) AS amount_usd,
    platform,
    A.blockchain,
    A._LOG_ID,
    A._INSERTED_TIMESTAMP
  FROM
    repayments_union A
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p
    ON A.token_address = p.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p.hour
),

{% if is_incremental() and var(
  'HEAL_MODEL'
) %}
heal_model AS (
  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    t0.contract_address,
    event_name,
    protocol_market,
    payer,
    borrower,
    t0.token_address,
    t0.token_symbol,
    amount_unadj,
    amount,
    ROUND(
      amount * p.price,
      2
    ) AS amount_usd_heal,
    platform,
    t0.blockchain,
    t0._LOG_ID,
    t0._INSERTED_TIMESTAMP
  FROM
    {{ this }}
    t0
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p
    ON t0.token_address = p.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p.hour
  WHERE
    CONCAT(
      t0.block_number,
      '-',
      t0.platform
    ) IN (
      SELECT
        CONCAT(
          t1.block_number,
          '-',
          t1.platform
        )
      FROM
        {{ this }}
        t1
      WHERE
        t1.amount_usd IS NULL
        AND t1._inserted_timestamp < (
          SELECT
            MAX(
              _inserted_timestamp
            ) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
          FROM
            {{ this }}
        )
        AND EXISTS (
          SELECT
            1
          FROM
            {{ ref('silver__complete_token_prices') }}
            p
          WHERE
            p._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
            AND p.price IS NOT NULL
            AND p.token_address = t1.token_address
            AND p.hour = DATE_TRUNC(
              'hour',
              t1.block_timestamp
            )
        )
      GROUP BY
        1
    )
),
{% endif %}

FINAL AS (
  SELECT
    *
  FROM
    complete_lending_repayments

{% if is_incremental() and var(
  'HEAL_MODEL'
) %}
UNION ALL
SELECT
  tx_hash,
  block_number,
  block_timestamp,
  event_index,
  origin_from_address,
  origin_to_address,
  origin_function_signature,
  contract_address,
  event_name,
  protocol_market,
  payer,
  borrower,
  token_address,
  token_symbol,
  amount_unadj,
  amount,
  amount_usd_heal AS amount_usd,
  platform,
  blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  heal_model
{% endif %}
)
SELECT
  *,
  {{ dbt_utils.generate_surrogate_key(['_log_id']) }}  AS complete_lending_repayments_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
  _inserted_timestamp DESC)) = 1