{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

    WITH oft_asset_contract_creation AS (

        SELECT
            block_number,
            block_timestamp,
            tx_hash,
            from_address AS oft_address,
            CASE
                WHEN tx_status = 'SUCCESS' THEN TRUE
                ELSE FALSE
            END AS tx_succeeded,
            CASE
                WHEN trace_status = 'SUCCESS' THEN TRUE
                ELSE FALSE
            END AS trace_succeeded,
            modified_timestamp
        FROM
            {{ ref('core__fact_traces') }}
        WHERE
            TYPE = 'CALL'
            AND LEFT(
                input,
                10
            ) = '0xca5eb5e1'
            AND to_address = '0x1a44076050125825900e736c501f859c50fe728c' -- layerzero v2
            AND tx_succeeded
            AND trace_succeeded

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY oft_address
    ORDER BY
        block_timestamp DESC
) = 1
),
oft_asset_base_token AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        from_address AS wrap_address,
        to_address AS underlying_address,
        CASE
            WHEN tx_status = 'SUCCESS' THEN TRUE
            ELSE FALSE
        END AS tx_succeeded,
        CASE
            WHEN trace_status = 'SUCCESS' THEN TRUE
            ELSE FALSE
        END AS trace_succeeded,
        modified_timestamp
    FROM
        {{ref('core__fact_traces')}}
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                contract_creation
        )
        AND TYPE = 'STATICCALL'
        AND input = '0x313ce567' qualify ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                trace_index ASC
        ) = 1
),
oft_asset AS (
SELECT
    oft_address,
    t3.token_symbol AS oft_symbol,
    t3.token_name AS oft_name,
    underlying_address,
    t4.token_symbol AS underlying_symbol,
    t4.token_name AS underlying_name
FROM
    oft_asset_contract_creation t1
    LEFT JOIN oft_asset_base_token t2
    ON t1.tx_hash = t2.tx_hash
    AND oft_address = wrap_address
    LEFT JOIN {{ref('silver__contracts')}} t3
    ON oft_address = t3.contract_address
    LEFT JOIN {{ref('silver__contracts')}} t4
    ON underlying_address = t4.contract_address
),
oft_sent AS (
    -- bridging transactions
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        {# '' AS event_name, #}
        'layerzero-v2' AS platform,
        oft_name,
        oft_symbol,
        oft_address,
        underlying_name,
        underlying_symbol,
        underlying_address,
        CONCAT('0x', SUBSTR(topics [2], 27, 40)) AS from_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: FLOAT AS dstEid,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: FLOAT AS amountSentLD,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) :: FLOAT AS amountReceivedLD,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ref('core__fact_event_logs')}}
        INNER JOIN oft_asset
        ON oft_address = contract_address
    WHERE
        topics [0] = '0x85496b760a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a'
        {# AND DATE(block_timestamp) >= '2024-11-01' #}
)
SELECT
    *
FROM
    oft_sent 
    -- inner join chain_id using(dstEid)
