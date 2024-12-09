{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH base_evt AS (

    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        contract_address,
        event_index,
        topics [0] :: STRING AS topic_0,
        topics [1] :: STRING AS topic_1,
        topics [2] :: STRING AS topic_2,
        topics [3] :: STRING AS topic_3,
        DATA,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CASE
            WHEN tx_status = 'success' THEN TRUE
            ELSE FALSE
        END AS tx_succeeded,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address = '0x55769baf6ec39b3bf4aae948eb890ea33307ef3c'
        AND topic_0 IN (
            '0x91f25e9be0134ec851830e0e76dc71e06f9dade75a9b84e9524071dbbc319425',
            -- TokenRedeemAndSwap
            '0x9a7024cde1920aa50cdde09ca396229e8c4d530d5cfdc6233590def70a94408c',
            -- TokenRedeemAndRemove
            '0xdc5bad4651c5fbe9977a696aadc65996c468cde1448dd468ec0d83bf61c4b57c' -- TokenRedeem
        )
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
redeem_swap AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        contract_address,
        event_index,
        'TokenRedeemAndSwap' AS event_name,
        topic_1 AS to_address,
        segmented_data,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) AS chainId,
        CONCAT('0x', SUBSTR(segmented_data [1], 25, 40)) AS token,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [2] :: STRING
            )
        ) AS amount,
        utils.udf_hex_to_int(
            segmented_data [3]
        ) AS tokenIndexFrom,
        -- source chain token index
        utils.udf_hex_to_int(
            segmented_data [4]
        ) AS tokenIndexTo,
        -- dst chain token index
        utils.udf_hex_to_int(
            segmented_data [5]
        ) AS minDy,
        utils.udf_hex_to_int(
            segmented_data [6]
        ) AS deadline,
        -- timestamp
        tx_succeeded,
        _log_id,
        modified_timestamp
    FROM
        base_evt
    WHERE
        topic_0 = '0x91f25e9be0134ec851830e0e76dc71e06f9dade75a9b84e9524071dbbc319425' -- TokenRedeemAndSwap
),
redeem_remove AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        contract_address,
        event_index,
        'TokenRedeemAndRemove' AS event_name,
        topic_1 AS to_address,
        segmented_data,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) AS chainId,
        CONCAT('0x', SUBSTR(segmented_data [1], 25, 40)) AS token,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [2] :: STRING
            )
        ) AS amount,
        utils.udf_hex_to_int(
            segmented_data [3]
        ) AS swapTokenIndex,
        -- dst chain token index
        utils.udf_hex_to_int(
            segmented_data [4]
        ) AS swapMinAmount,
        utils.udf_hex_to_int(
            segmented_data [5]
        ) AS swapDeadline,
        -- timestamp
        tx_succeeded,
        _log_id,
        modified_timestamp
    FROM
        base_evt
    WHERE
        topic_0 = '0x9a7024cde1920aa50cdde09ca396229e8c4d530d5cfdc6233590def70a94408c' -- TokenRedeemAndRemove
),
redeem_only AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        contract_address,
        event_index,
        'TokenRedeem' AS event_name,
        topic_1 AS to_address,
        segmented_data,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) AS chainId,
        CONCAT('0x', SUBSTR(segmented_data [1], 25, 40)) AS token,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [2] :: STRING
            )
        ) AS amount,
        tx_succeeded,
        _log_id,
        modified_timestamp
    FROM
        base_evt
    WHERE
        topic_0 = '0xdc5bad4651c5fbe9977a696aadc65996c468cde1448dd468ec0d83bf61c4b57c' -- TokenRedeem
),
all_evts AS (
    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        event_name,
        event_removed,
        tx_status,
        contract_address AS bridge_address,
        amount,
        origin_from_address AS sender,
        to_address AS receiver,
        receiver AS destination_chain_receiver,
        chainId AS destination_chain_id,
        token AS token_address,
    FROM
        redeem_swap
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        event_name,
        event_removed,
        tx_status,
        contract_address AS bridge_address,
        amount,
        origin_from_address AS sender,
        to_address AS receiver,
        receiver AS destination_chain_receiver,
        chainId AS destination_chain_id,
        token AS token_address,
    FROM
        redeem_remove
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        event_name,
        event_removed,
        tx_status,
        contract_address AS bridge_address,
        amount,
        origin_from_address AS sender,
        to_address AS receiver,
        receiver AS destination_chain_receiver,
        chainId AS destination_chain_id,
        token AS token_address,
    FROM
        redeem_only
)
SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    event_name,
    event_removed,
    tx_status,
    bridge_address,
    'synapse' AS platform,
    amount,
    sender,
    receiver,
    destination_chain_receiver,
    destination_chain_id,
    token_address,
    _log_id,
    modified_timestamp
FROM
    all_evts
