{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH swaps_base AS (

    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        l.event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        l.contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS amount0In,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS amount1In,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [2] :: STRING
            )
        ) AS amount0Out,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [3] :: STRING
            )
        ) AS amount1Out,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS sender,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS tx_to,
        token0,
        token1,
        CONCAT(
            l.tx_hash :: STRING,
            '-',
            l.event_index :: STRING
        ) AS _log_id,
        l.modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
        INNER JOIN {{ ref('silver_dex__blaster_pools') }}
        p
        ON p.pool_address = l.contract_address
    WHERE
        topics [0] :: STRING = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822'
        AND tx_succeeded

{% if is_incremental() %}
AND l.modified_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    sender,
    tx_to,
    amount0In,
    amount1In,
    amount0Out,
    amount1Out,
    token0,
    token1,
    CASE
        WHEN amount0In <> 0
        AND amount1In <> 0
        AND amount0Out <> 0 THEN amount1In
        WHEN amount0In <> 0 THEN amount0In
        WHEN amount1In <> 0 THEN amount1In
    END AS amount_in_unadj,
    CASE
        WHEN amount0Out <> 0 THEN amount0Out
        WHEN amount1Out <> 0 THEN amount1Out
    END AS amount_out_unadj,
    CASE
        WHEN amount0In <> 0
        AND amount1In <> 0
        AND amount0Out <> 0 THEN token1
        WHEN amount0In <> 0 THEN token0
        WHEN amount1In <> 0 THEN token1
    END AS token_in,
    CASE
        WHEN amount0Out <> 0 THEN token0
        WHEN amount1Out <> 0 THEN token1
    END AS token_out,
    'Swap' AS event_name,
    'blasterswap' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    swaps_base
WHERE
    token_in <> token_out
