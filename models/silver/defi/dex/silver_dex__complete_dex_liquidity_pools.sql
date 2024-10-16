{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['block_number','platform','version'],
  cluster_by = ['block_timestamp::DATE','platform'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, contract_address, pool_address, pool_name, tokens, symbols), SUBSTRING(pool_address, pool_name, tokens, symbols)",
  tags = ['curated','reorg','heal']
) }}

WITH contracts AS (

  SELECT
    contract_address,
    token_symbol,
    token_decimals,
    _inserted_timestamp
  FROM
    {{ ref('silver__contracts') }}
),
bladeswap AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    pool_address,
    NULL AS pool_name,
    NULL AS fee,
    NULL AS tick_spacing,
    token0,
    token1,
    NULL AS token2,
    NULL AS token3,
    NULL AS token4,
    NULL AS token5,
    NULL AS token6,
    NULL AS token7,
    'bladeswap' AS platform,
    'v1' AS version,
    _log_id AS _id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__bladeswap_pools') }}

{% if is_incremental() and 'bladeswap' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
bladeswap_v3 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    pool_address,
    NULL AS pool_name,
    fee,
    tick_spacing,
    token0_address AS token0,
    token1_address AS token1,
    NULL AS token2,
    NULL AS token3,
    NULL AS token4,
    NULL AS token5,
    NULL AS token6,
    NULL AS token7,
    'bladeswap-v3' AS platform,
    'v3' AS version,
    _log_id AS _id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__bladeswap_pools_v3') }}

{% if is_incremental() and 'bladeswap_v3' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
blaster AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    pool_address,
    NULL AS pool_name,
    NULL AS fee,
    NULL AS tick_spacing,
    token0,
    token1,
    NULL AS token2,
    NULL AS token3,
    NULL AS token4,
    NULL AS token5,
    NULL AS token6,
    NULL AS token7,
    'blasterswap' AS platform,
    'v1' AS version,
    _log_id AS _id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__blaster_pools') }}

{% if is_incremental() and 'blaster' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
blaster_v3 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    pool_address,
    NULL AS pool_name,
    fee,
    tick_spacing,
    token0_address AS token0,
    token1_address AS token1,
    NULL AS token2,
    NULL AS token3,
    NULL AS token4,
    NULL AS token5,
    NULL AS token6,
    NULL AS token7,
    'blasterswap-v3' AS platform,
    'v3' AS version,
    _log_id AS _id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__blaster_pools_v3') }}

{% if is_incremental() and 'blaster_v3' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
fenix_v3 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    pool_address,
    NULL AS pool_name,
    fee,
    tick_spacing,
    token0_address AS token0,
    token1_address AS token1,
    NULL AS token2,
    NULL AS token3,
    NULL AS token4,
    NULL AS token5,
    NULL AS token6,
    NULL AS token7,
    'fenix-v3' AS platform,
    'v3' AS version,
    _log_id AS _id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__fenix_pools_v3') }}

{% if is_incremental() and 'fenix_v3' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
ring AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    pool_address,
    NULL AS pool_name,
    NULL AS fee,
    NULL AS tick_spacing,
    token0,
    token1,
    NULL AS token2,
    NULL AS token3,
    NULL AS token4,
    NULL AS token5,
    NULL AS token6,
    NULL AS token7,
    'ring' AS platform,
    'v1' AS version,
    _log_id AS _id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__ring_pools') }}

{% if is_incremental() and 'ring' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
ring_v3 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    pool_address,
    NULL AS pool_name,
    fee,
    tick_spacing,
    token0_address AS token0,
    token1_address AS token1,
    NULL AS token2,
    NULL AS token3,
    NULL AS token4,
    NULL AS token5,
    NULL AS token6,
    NULL AS token7,
    'ring-v3' AS platform,
    'v3' AS version,
    _log_id AS _id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__ring_pools_v3') }}

{% if is_incremental() and 'ring_v3' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
sushi AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    pool_address,
    NULL AS pool_name,
    NULL AS fee,
    NULL AS tick_spacing,
    token0,
    token1,
    NULL AS token2,
    NULL AS token3,
    NULL AS token4,
    NULL AS token5,
    NULL AS token6,
    NULL AS token7,
    'sushiswap' AS platform,
    'v1' AS version,
    _log_id AS _id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__sushi_pools') }}

{% if is_incremental() and 'sushi' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
sushi_v3 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    pool_address,
    NULL AS pool_name,
    fee,
    tick_spacing,
    token0_address AS token0,
    token1_address AS token1,
    NULL AS token2,
    NULL AS token3,
    NULL AS token4,
    NULL AS token5,
    NULL AS token6,
    NULL AS token7,
    'sushiswap-v3' AS platform,
    'v3' AS version,
    _log_id AS _id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__sushi_pools_v3') }}

{% if is_incremental() and 'sushi_v3' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
thruster AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    pool_address,
    NULL AS pool_name,
    NULL AS fee,
    NULL AS tick_spacing,
    token0,
    token1,
    NULL AS token2,
    NULL AS token3,
    NULL AS token4,
    NULL AS token5,
    NULL AS token6,
    NULL AS token7,
    'thruster' AS platform,
    'v1' AS version,
    _log_id AS _id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__thruster_pools') }}

{% if is_incremental() and 'thruster' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
thruster_v3 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    pool_address,
    NULL AS pool_name,
    fee,
    tick_spacing,
    token0_address AS token0,
    token1_address AS token1,
    NULL AS token2,
    NULL AS token3,
    NULL AS token4,
    NULL AS token5,
    NULL AS token6,
    NULL AS token7,
    'thruster-v3' AS platform,
    'v3' AS version,
    _log_id AS _id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__thruster_pools_v3') }}

{% if is_incremental() and 'thruster_v3' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
all_pools AS (
  SELECT
    *
  FROM
    bladeswap
  UNION ALL
  SELECT
    *
  FROM
    bladeswap_v3
  UNION ALL
  SELECT
    *
  FROM
    blaster
  UNION ALL
  SELECT
    *
  FROM
    blaster_v3
  UNION ALL
  SELECT
    *
  FROM
    fenix_v3
  UNION ALL
  SELECT
    *
  FROM
    ring
  UNION ALL
  SELECT
    *
  FROM
    ring_v3
  UNION ALL
  SELECT
    *
  FROM
    sushi
  UNION ALL
  SELECT
    *
  FROM
    sushi_v3
  UNION ALL
  SELECT
    *
  FROM
    thruster
  UNION ALL
  SELECT
    *
  FROM
    thruster_v3
),
complete_lps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    p.contract_address,
    pool_address,
    CASE
      WHEN pool_name IS NOT NULL THEN pool_name
      WHEN pool_name IS NULL
      AND platform IN (
        'bladeswap-v3',
        'blasterswap-v3',
        'fenix-v3',
        'ring-v3',
        'sushiswap-v3',
        'thruster-v3'
      ) THEN CONCAT(
        COALESCE(
          c0.token_symbol,
          CONCAT(SUBSTRING(token0, 1, 5), '...', SUBSTRING(token0, 39, 42))
        ),
        '-',
        COALESCE(
          c1.token_symbol,
          CONCAT(SUBSTRING(token1, 1, 5), '...', SUBSTRING(token1, 39, 42))
        ),
        ' ',
        COALESCE(
          fee,
          0
        ),
        ' ',
        COALESCE(
          tick_spacing,
          0
        ),
        CASE
          WHEN platform = 'bladeswap-v3' THEN ' BLP'
          WHEN platform = 'blasterswap-v3' THEN ' BLP'
          WHEN platform = 'fenix-v3' THEN ' FLP'
          WHEN platform = 'ring-v3' THEN ' RLP'
          WHEN platform = 'sushiswap-v3' THEN ' SLP'
          WHEN platform = 'thruster-v3' THEN ' TLP'
        END
      )
      ELSE CONCAT(
        COALESCE(
          c0.token_symbol,
          CONCAT(SUBSTRING(token0, 1, 5), '...', SUBSTRING(token0, 39, 42))
        ),
        '-',
        COALESCE(
          c1.token_symbol,
          CONCAT(SUBSTRING(token1, 1, 5), '...', SUBSTRING(token1, 39, 42))
        )
      )
    END AS pool_name,
    fee,
    tick_spacing,
    token0,
    token1,
    token2,
    token3,
    token4,
    token5,
    token6,
    token7,
    OBJECT_CONSTRUCT(
      'token0',
      token0,
      'token1',
      token1,
      'token2',
      token2,
      'token3',
      token3,
      'token4',
      token4,
      'token5',
      token5,
      'token6',
      token6,
      'token7',
      token7
    ) AS tokens,
    OBJECT_CONSTRUCT(
      'token0',
      c0.token_symbol,
      'token1',
      c1.token_symbol,
      'token2',
      c2.token_symbol,
      'token3',
      c3.token_symbol,
      'token4',
      c4.token_symbol,
      'token5',
      c5.token_symbol,
      'token6',
      c6.token_symbol,
      'token7',
      c7.token_symbol
    ) AS symbols,
    OBJECT_CONSTRUCT(
      'token0',
      c0.token_decimals,
      'token1',
      c1.token_decimals,
      'token2',
      c2.token_decimals,
      'token3',
      c3.token_decimals,
      'token4',
      c4.token_decimals,
      'token5',
      c5.token_decimals,
      'token6',
      c6.token_decimals,
      'token7',
      c7.token_decimals
    ) AS decimals,
    platform,
    version,
    _id,
    p._inserted_timestamp
  FROM
    all_pools p
    LEFT JOIN contracts c0
    ON c0.contract_address = p.token0
    LEFT JOIN contracts c1
    ON c1.contract_address = p.token1
    LEFT JOIN contracts c2
    ON c2.contract_address = p.token2
    LEFT JOIN contracts c3
    ON c3.contract_address = p.token3
    LEFT JOIN contracts c4
    ON c4.contract_address = p.token4
    LEFT JOIN contracts c5
    ON c5.contract_address = p.token5
    LEFT JOIN contracts c6
    ON c6.contract_address = p.token6
    LEFT JOIN contracts c7
    ON c7.contract_address = p.token7
),

{% if is_incremental() and var(
  'HEAL_MODEL'
) %}
heal_model AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    t0.contract_address,
    pool_address,
    CASE
      WHEN pool_name IS NOT NULL THEN pool_name
      WHEN pool_name IS NULL
      AND platform IN (
        'bladeswap-v3',
        'blasterswap-v3',
        'fenix-v3',
        'ring-v3',
        'sushiswap-v3',
        'thruster-v3'
      ) THEN CONCAT(
        COALESCE(
          c0.token_symbol,
          CONCAT(SUBSTRING(token0, 1, 5), '...', SUBSTRING(token0, 39, 42))
        ),
        '-',
        COALESCE(
          c1.token_symbol,
          CONCAT(SUBSTRING(token1, 1, 5), '...', SUBSTRING(token1, 39, 42))
        ),
        ' ',
        COALESCE(
          fee,
          0
        ),
        ' ',
        COALESCE(
          tick_spacing,
          0
        ),
        CASE
          WHEN platform = 'bladeswap-v3' THEN ' BLP'
          WHEN platform = 'blasterswap-v3' THEN ' BLP'
          WHEN platform = 'fenix-v3' THEN ' FLP'
          WHEN platform = 'ring-v3' THEN ' RLP'
          WHEN platform = 'sushiswap-v3' THEN ' SLP'
          WHEN platform = 'thruster-v3' THEN ' TLP'
        END
      )
      ELSE CONCAT(
        COALESCE(
          c0.token_symbol,
          CONCAT(SUBSTRING(token0, 1, 5), '...', SUBSTRING(token0, 39, 42))
        ),
        '-',
        COALESCE(
          c1.token_symbol,
          CONCAT(SUBSTRING(token1, 1, 5), '...', SUBSTRING(token1, 39, 42))
        )
      )
    END AS pool_name_heal,
    fee,
    tick_spacing,
    token0,
    token1,
    token2,
    token3,
    token4,
    token5,
    token6,
    token7,
    tokens,
    OBJECT_CONSTRUCT(
      'token0',
      c0.token_symbol,
      'token1',
      c1.token_symbol,
      'token2',
      c2.token_symbol,
      'token3',
      c3.token_symbol,
      'token4',
      c4.token_symbol,
      'token5',
      c5.token_symbol,
      'token6',
      c6.token_symbol,
      'token7',
      c7.token_symbol
    ) AS symbols_heal,
    OBJECT_CONSTRUCT(
      'token0',
      c0.token_decimals,
      'token1',
      c1.token_decimals,
      'token2',
      c2.token_decimals,
      'token3',
      c3.token_decimals,
      'token4',
      c4.token_decimals,
      'token5',
      c5.token_decimals,
      'token6',
      c6.token_decimals,
      'token7',
      c7.token_decimals
    ) AS decimals_heal,
    platform,
    version,
    _id,
    t0._inserted_timestamp
  FROM
    {{ this }}
    t0
    LEFT JOIN contracts c0
    ON c0.contract_address = t0.token0
    LEFT JOIN contracts c1
    ON c1.contract_address = t0.token1
    LEFT JOIN contracts c2
    ON c2.contract_address = t0.token2
    LEFT JOIN contracts c3
    ON c3.contract_address = t0.token3
    LEFT JOIN contracts c4
    ON c4.contract_address = t0.token4
    LEFT JOIN contracts c5
    ON c5.contract_address = t0.token5
    LEFT JOIN contracts c6
    ON c6.contract_address = t0.token6
    LEFT JOIN contracts c7
    ON c7.contract_address = t0.token7
  WHERE
    CONCAT(
      t0.block_number,
      '-',
      t0.platform,
      '-',
      t0.version
    ) IN (
      SELECT
        CONCAT(
          t1.block_number,
          '-',
          t1.platform,
          '-',
          t1.version
        )
      FROM
        {{ this }}
        t1
      WHERE
        t1.decimals :token0 :: INT IS NULL
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
            {{ ref('silver__contracts') }} C
          WHERE
            C._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
            AND C.token_decimals IS NOT NULL
            AND C.contract_address = t1.tokens :token0 :: STRING)
          GROUP BY
            1
        )
        OR CONCAT(
          t0.block_number,
          '-',
          t0.platform,
          '-',
          t0.version
        ) IN (
          SELECT
            CONCAT(
              t2.block_number,
              '-',
              t2.platform,
              '-',
              t2.version
            )
          FROM
            {{ this }}
            t2
          WHERE
            t2.decimals :token1 :: INT IS NULL
            AND t2._inserted_timestamp < (
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
                {{ ref('silver__contracts') }} C
              WHERE
                C._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
                AND C.token_decimals IS NOT NULL
                AND C.contract_address = t2.tokens :token1 :: STRING)
              GROUP BY
                1
            )
            OR CONCAT(
              t0.block_number,
              '-',
              t0.platform,
              '-',
              t0.version
            ) IN (
              SELECT
                CONCAT(
                  t3.block_number,
                  '-',
                  t3.platform,
                  '-',
                  t3.version
                )
              FROM
                {{ this }}
                t3
              WHERE
                t3.decimals :token2 :: INT IS NULL
                AND t3._inserted_timestamp < (
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
                    {{ ref('silver__contracts') }} C
                  WHERE
                    C._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
                    AND C.token_decimals IS NOT NULL
                    AND C.contract_address = t3.tokens :token2 :: STRING)
                  GROUP BY
                    1
                )
                OR CONCAT(
                  t0.block_number,
                  '-',
                  t0.platform,
                  '-',
                  t0.version
                ) IN (
                  SELECT
                    CONCAT(
                      t4.block_number,
                      '-',
                      t4.platform,
                      '-',
                      t4.version
                    )
                  FROM
                    {{ this }}
                    t4
                  WHERE
                    t4.decimals :token3 :: INT IS NULL
                    AND t4._inserted_timestamp < (
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
                        {{ ref('silver__contracts') }} C
                      WHERE
                        C._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
                        AND C.token_decimals IS NOT NULL
                        AND C.contract_address = t4.tokens :token3 :: STRING)
                      GROUP BY
                        1
                    )
                    OR CONCAT(
                      t0.block_number,
                      '-',
                      t0.platform,
                      '-',
                      t0.version
                    ) IN (
                      SELECT
                        CONCAT(
                          t5.block_number,
                          '-',
                          t5.platform,
                          '-',
                          t5.version
                        )
                      FROM
                        {{ this }}
                        t5
                      WHERE
                        t5.decimals :token4 :: INT IS NULL
                        AND t5._inserted_timestamp < (
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
                            {{ ref('silver__contracts') }} C
                          WHERE
                            C._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
                            AND C.token_decimals IS NOT NULL
                            AND C.contract_address = t5.tokens :token4 :: STRING)
                          GROUP BY
                            1
                        )
                        OR CONCAT(
                          t0.block_number,
                          '-',
                          t0.platform,
                          '-',
                          t0.version
                        ) IN (
                          SELECT
                            CONCAT(
                              t6.block_number,
                              '-',
                              t6.platform,
                              '-',
                              t6.version
                            )
                          FROM
                            {{ this }}
                            t6
                          WHERE
                            t6.decimals :token5 :: INT IS NULL
                            AND t6._inserted_timestamp < (
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
                                {{ ref('silver__contracts') }} C
                              WHERE
                                C._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
                                AND C.token_decimals IS NOT NULL
                                AND C.contract_address = t6.tokens :token5 :: STRING)
                              GROUP BY
                                1
                            )
                            OR CONCAT(
                              t0.block_number,
                              '-',
                              t0.platform,
                              '-',
                              t0.version
                            ) IN (
                              SELECT
                                CONCAT(
                                  t7.block_number,
                                  '-',
                                  t7.platform,
                                  '-',
                                  t7.version
                                )
                              FROM
                                {{ this }}
                                t7
                              WHERE
                                t7.decimals :token6 :: INT IS NULL
                                AND t7._inserted_timestamp < (
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
                                    {{ ref('silver__contracts') }} C
                                  WHERE
                                    C._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
                                    AND C.token_decimals IS NOT NULL
                                    AND C.contract_address = t7.tokens :token6 :: STRING)
                                  GROUP BY
                                    1
                                )
                                OR CONCAT(
                                  t0.block_number,
                                  '-',
                                  t0.platform,
                                  '-',
                                  t0.version
                                ) IN (
                                  SELECT
                                    CONCAT(
                                      t8.block_number,
                                      '-',
                                      t8.platform,
                                      '-',
                                      t8.version
                                    )
                                  FROM
                                    {{ this }}
                                    t8
                                  WHERE
                                    t8.decimals :token7 :: INT IS NULL
                                    AND t8._inserted_timestamp < (
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
                                        {{ ref('silver__contracts') }} C
                                      WHERE
                                        C._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
                                        AND C.token_decimals IS NOT NULL
                                        AND C.contract_address = t8.tokens :token7 :: STRING)
                                      GROUP BY
                                        1
                                    )
                                ),
                              {% endif %}

                              FINAL AS (
                                SELECT
                                  *
                                FROM
                                  complete_lps

{% if is_incremental() and var(
  'HEAL_MODEL'
) %}
UNION ALL
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  contract_address,
  pool_address,
  pool_name_heal AS pool_name,
  fee,
  tick_spacing,
  token0,
  token1,
  token2,
  token3,
  token4,
  token5,
  token6,
  token7,
  tokens,
  symbols_heal AS symbols,
  decimals_heal AS decimals,
  platform,
  version,
  _id,
  _inserted_timestamp
FROM
  heal_model
{% endif %}
)
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  platform,
  version,
  contract_address,
  pool_address,
  pool_name,
  tokens,
  symbols,
  decimals,
  fee,
  tick_spacing,
  token0,
  token1,
  token2,
  token3,
  token4,
  token5,
  token6,
  token7,
  _id,
  _inserted_timestamp,
  {{ dbt_utils.generate_surrogate_key(
    ['pool_address']
  ) }} AS complete_dex_liquidity_pools_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  FINAL
