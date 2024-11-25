{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH swaps_base AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CASE
            WHEN topics [1] :: STRING = '0x0000000000000000000000000000000000000000000000000000000000000000' THEN '0x4300000000000000000000000000000000000004'
            ELSE CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40))
        END AS base_token,
        CASE
            WHEN topics [2] :: STRING = '0x0000000000000000000000000000000000000000000000000000000000000000' THEN '0x4300000000000000000000000000000000000004'
            ELSE CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))
        END AS quote_token,
        TRY_TO_BOOLEAN(RIGHT(segmented_data [1], 1)) AS isBuy,
        -- TRUE when user pays in base token, FALSE when user pays in quote token
        TRY_TO_BOOLEAN(RIGHT(segmented_data [2], 1)) AS inBaseQty,
        -- TRUE when fixed qty is in base token, FALSE when fixed qty is in quote token
        utils.udf_hex_to_int(
            's2c',
            segmented_data [3] :: STRING
        ) :: FLOAT AS qty,
        -- in qty for fixed qty in swap or out qty for fixed qty out swap
        utils.udf_hex_to_int(
            's2c',
            segmented_data [6] :: STRING
        ) :: FLOAT AS non_fixed_qty,
        --minOut for fixed qty in swaps or maxIn for fixed qty out swaps
        utils.udf_hex_to_int(
            's2c',
            segmented_data [8] :: STRING
        ) :: FLOAT AS base_qty,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [9] :: STRING
        ) :: FLOAT AS quote_qty,
        CASE
            WHEN base_qty > 0 THEN base_qty
            WHEN quote_qty > 0 THEN quote_qty
            ELSE NULL
        END AS amount_in_unadj,
        CASE
            WHEN base_qty > 0 THEN ABS(quote_qty)
            WHEN quote_qty > 0 THEN ABS(base_qty)
            ELSE NULL
        END AS amount_out_unadj,
        CASE
            WHEN base_qty > 0 THEN base_token
            WHEN quote_qty > 0 THEN quote_token
            ELSE NULL
        END AS token_in,
        CASE
            WHEN base_qty > 0 THEN quote_token
            WHEN quote_qty > 0 THEN base_token
            ELSE NULL
        END AS token_out,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        contract_address = '0xaaaaaaaaffe404ee9433eef0094b6382d81fb958' --CrocSwapDex
        AND topics [0] :: STRING = '0x5d7a6c346454f5c536b7f52655e780f6db27b15b489f80f2dbb288c9e4f366bd'
        AND tx_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
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
    origin_from_address AS sender,
    CASE
        WHEN origin_function_signature = '0xa62ea46d' THEN origin_to_address
        ELSE origin_from_address
    END AS tx_to,
    token_in,
    token_out,
    amount_in_unadj,
    amount_out_unadj,
    'Swap' AS event_name,
    'ambient' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    swaps_base
WHERE
    token_in IS NOT NULL
    AND token_out IS NOT NULL
