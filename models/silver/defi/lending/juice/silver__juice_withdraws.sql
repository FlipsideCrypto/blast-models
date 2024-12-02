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
withdraw_logs AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        DATA,
        topics,
        tx_status,
        _inserted_timestamp,
        _log_id
    FROM
        {{ ref('silver__logs') }}
    WHERE
        (
            contract_address IN (
                SELECT
                    pool_address
                FROM
                    asset_details
            )
            OR contract_address IN (
                SELECT
                    token_address
                FROM
                    asset_details
            )
        )
        AND topics [0] :: STRING IN (
            '0x884edad9ce6fa2440d8a54cc123490eb96d2768479d49ff9c7366125a9424364' -- pool redeem,
            '0x0b260cc77140cab3405675836fc971314e656137208b77414be51fafd58ae34b' -- collateral redeem,
            '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' -- transfer
        )
),
juice_redemption AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        contract_address AS token,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS received_amount_raw,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS redeemer,
        'Juice' AS platform,
        _inserted_timestamp,
        _log_id
    FROM
        withdraw_logs
    WHERE
        contract_address IN (
            SELECT
                pool_address
            FROM
                asset_details
        )
        AND topics [0] :: STRING = '0x0b260cc77140cab3405675836fc971314e656137208b77414be51fafd58ae34b'
        AND tx_status = 'SUCCESS'
),
juice_collateralredeems AS (
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
        regexp_substr_all(SUBSTR(DATA :: STRING, 3, len(DATA)), '.{64}') AS segmented_data,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) :: INTEGER AS received_amount_raw,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS redeemer,
        'Juice' AS platform,
        inserted_timestamp AS _inserted_timestamp,
        _log_id
    FROM
        withdraw_logs
    WHERE
        contract_address IN (
            SELECT
                token_address
            FROM
                asset_details
        )
        AND topics [0] :: STRING = '0x0b260cc77140cab3405675836fc971314e656137208b77414be51fafd58ae34b'
        AND tx_status = 'SUCCESS'
),
token_transfer AS (
    SELECT
        block_timestamp,
        tx_hash,
        utils.udf_hex_to_int(DATA) AS redeemed_token_raw
    FROM
        withdraw_logs
    WHERE
        1 = 1
        AND contract_address IN (
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
                juice_redemption
            UNION ALL
            SELECT
                tx_hash
            FROM
                juice_collateralredeems
        )
        AND tx_status = 'SUCCESS'
),
juice_combine AS (
    SELECT
        b.block_number,
        b.block_timestamp,
        b.tx_hash,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        redeemer,
        received_amount_raw,
        redeemed_token_raw,
        C.underlying_asset_address AS received_contract_address,
        C.underlying_symbol AS received_symbol,
        C.token_symbol,
        C.token_decimals,
        C.underlying_decimals,
        b.platform,
        b._log_id,
        b._inserted_timestamp
    FROM
        juice_redemption b
        LEFT JOIN token_transfer USING(tx_hash)
        LEFT JOIN asset_details C
        ON b.token = C.pool_address
    UNION ALL
    SELECT
        b.block_number,
        b.block_timestamp,
        b.tx_hash,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        redeemer,
        received_amount_raw,
        redeemed_token_raw,
        C.underlying_asset_address AS received_contract_address,
        C.underlying_symbol AS received_symbol,
        C.token_symbol,
        C.token_decimals,
        C.underlying_decimals,
        b.platform,
        b._log_id,
        b._inserted_timestamp
    FROM
        juice_collateralredeems b
        LEFT JOIN token_transfer d USING(tx_hash)
        LEFT JOIN asset_details C
        ON b.token_address = C.token_address
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
    --token as token_address,
    token_symbol,
    received_amount_raw AS amount_unadj,
    received_amount_raw / pow(
        10,
        underlying_decimals
    ) AS amount,
    received_contract_address,
    received_symbol,
    redeemed_token_raw / pow(
        10,
        token_decimals
    ) AS redeemed_tokens,
    redeemer,
    platform,
    _inserted_timestamp,
    _log_id
FROM
    juice_combine ee qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
