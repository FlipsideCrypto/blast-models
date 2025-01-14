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
init_redemption AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        contract_address AS token_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        utils.udf_hex_to_int(
            topics [1] :: STRING
        ) :: FLOAT AS posId,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS pool,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: FLOAT AS redeemed_token_raw,
        'INIT Capital' AS platform,
        modified_timestamp,
        _log_id
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address = '0xa7d36f2106b5a5d528a7e2e7a3f436d703113a10'
        AND topics [0] :: STRING = '0x09c2e7b3728acfd99b3f71e4c1a55bcd48019bcc0e45c741f7c2f3393f49ea91'
        AND tx_status = 'SUCCESS'

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
token_transfer1 AS (
    SELECT
        t1.tx_hash,
        t2.contract_address,
        t1.from_address,
        t1.to_address,
        t1.raw_amount,
        t3.token_symbol,
        t3.token_decimals,
        t3.token_name,
        t2.from_address AS from_address2,
        t2.to_address AS to_address2,
        t2.raw_amount AS base_amount
    FROM
        {{ ref('core__fact_token_transfers') }}
        t1
        LEFT JOIN {{ ref('core__fact_token_transfers') }}
        t2
        ON t1.tx_hash = t2.tx_hash
        AND t1.contract_address = t2.from_address
        LEFT JOIN {{ ref('silver__contracts') }}
        t3
        ON t2.contract_address = t3.contract_address
    WHERE
        1 = 1
        AND t1.contract_address IN (
            SELECT
                underlying_asset_address
            FROM
                asset_details
            WHERE
                underlying_unwrap_address IS NOT NULL
        )
        AND t1.tx_hash IN (
            SELECT
                tx_hash
            FROM
                init_redemption
        )
        AND t1.from_address IN (
            SELECT
                token_address
            FROM
                asset_details
        )
        AND t2.contract_address IN (
            SELECT
                underlying_unwrap_address
            FROM
                asset_details
        )
        AND (
            t2.from_address IN (
                SELECT
                    underlying_asset_address
                FROM
                    asset_details
            )
        )
),
token_transfer2 AS (
    SELECT
        t1.tx_hash,
        t1.contract_address,
        t1.from_address,
        t1.to_address,
        t1.raw_amount,
        t3.token_symbol,
        t3.token_decimals,
        t3.token_name,
        NULL AS from_address2,
        NULL AS to_address2,
        NULL AS base_amount,
        NULL AS base_decimals,
        NULL AS base_symbol,
        NULL AS base_name
    FROM
        {{ ref('core__fact_token_transfers') }}
        t1
        LEFT JOIN {{ ref('silver__contracts') }}
        t3
        ON t1.contract_address = t3.contract_address
    WHERE
        1 = 1
        AND t1.contract_address IN (
            SELECT
                underlying_asset_address
            FROM
                asset_details
            WHERE
                underlying_unwrap_address IS NULL
        )
        AND t1.tx_hash IN (
            SELECT
                tx_hash
            FROM
                init_redemption
        )
        {# AND t1.tx_hash NOT IN (
            SELECT
                tx_hash
            FROM
                token_transfer1
        ) #} --think we would want to remove this
        AND t1.from_address IN (
            SELECT
                token_address
            FROM
                asset_details
        )
),
token_transfer AS (
    SELECT
        tx_hash,
        contract_address,
        token_decimals,
        token_symbol,
        token_name,
        COALESCE(
            base_amount,
            raw_amount
        ) AS raw_amount,
        from_address
    FROM
        (
            SELECT
                tx_hash,
                contract_address,
                base_amount,
                raw_amount,
                from_address2,
                from_address,
                token_name,
                token_symbol,
                token_decimals
            FROM
                token_transfer1
            UNION ALL
            SELECT
                tx_hash,
                contract_address,
                base_amount,
                raw_amount,
                from_address2,
                from_address,
                token_name,
                token_symbol,
                token_decimals
            FROM
                token_transfer2
        )
),
native_transfer AS (
    SELECT
        tx_hash,
        from_address AS wrapped_address,
        to_address,
        value_precise_raw AS eth_value,
        'WETH' AS eth_symbol,
        18 AS eth_decimals,
        '0x4300000000000000000000000000000000000004' AS eth_address
    FROM
        blast.core.fact_traces
    WHERE
        from_address IN ('0xf683ce59521aa464066783d78e40cd9412f33d21')
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                init_redemption
        )
        AND TYPE = 'CALL'
        AND trace_status = 'SUCCESS'
        AND input = '0x'
),
init_combine AS (
    SELECT
        b.block_number,
        b.block_timestamp,
        b.tx_hash,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_to_address AS redeemer,
        origin_function_signature,
        b.contract_address,
        pool,
        COALESCE(
            eth_value,
            raw_amount
        ) AS received_amount_raw,
        COALESCE(
            eth_decimals,
            d.token_decimals
        ) AS received_decimals,
        COALESCE(
            eth_symbol,
            d.token_symbol
        ) AS received_symbol,
        COALESCE(
            eth_address,
            d.contract_address
        ) AS received_contract_address,
        redeemed_token_raw,
        C.token_address,
        C.token_symbol,
        C.token_decimals,
        C.token_address AS redeemed_contract_address,
        C.token_symbol AS redeemed_symbol,
        C.token_decimals AS redeemed_decimals,
        b.platform,
        b._log_id,
        b.modified_timestamp
    FROM
        init_redemption b
        LEFT JOIN asset_details C
        ON b.pool = C.token_address
        LEFT JOIN token_transfer d
        ON b.tx_hash = d.tx_hash
        AND b.pool = d.from_address
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
    token_address,
    token_symbol,
    redeemer,
    received_amount_raw AS amount_unadj,
    received_amount_raw / pow(
        10,
        received_decimals
    ) AS amount,
    received_contract_address,
    received_symbol,
    redeemed_token_raw / pow(
        10,
        redeemed_decimals
    ) AS redeemed_tokens,
    platform,
    modified_timestamp,
    _log_id
FROM
    init_combine ee qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    modified_timestamp DESC)) = 1