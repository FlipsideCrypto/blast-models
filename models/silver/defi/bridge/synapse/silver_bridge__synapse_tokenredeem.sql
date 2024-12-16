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
        topics,
        topics [0] :: STRING AS topic_0,
        topics [1] :: STRING AS topic_1,
        topics [2] :: STRING AS topic_2,
        topics [3] :: STRING AS topic_3,
        DATA,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CASE
            WHEN tx_status = 'SUCCESS' THEN TRUE
            ELSE FALSE
        END AS tx_succeeded,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address = '0x55769baf6ec39b3bf4aae948eb890ea33307ef3c'
        AND topic_0 IN (
            '0x91f25e9be0134ec851830e0e76dc71e06f9dade75a9b84e9524071dbbc319425',
            -- TokenRedeemAndSwap
            '0x79c15604b92ef54d3f61f0c40caab8857927ca3d5092367163b4562c1699eb5f',
            -- TokenDepositAndSwap
            '0x9a7024cde1920aa50cdde09ca396229e8c4d530d5cfdc6233590def70a94408c',
            -- TokenRedeemAndRemove
            '0xdc5bad4651c5fbe9977a696aadc65996c468cde1448dd468ec0d83bf61c4b57c',
            -- TokenRedeem
            '0xda5273705dbef4bf1b902a131c2eac086b7e1476a8ab0cb4da08af1fe1bd8e3b',
            -- TokenDeposit
            '0x8e57e8c5fea426159af69d47eda6c5052c7605c9f70967cf749d4aa55b70b499' -- TokenRedeemV2 (terra specific)
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
        CASE
            WHEN topic_0 = '0x91f25e9be0134ec851830e0e76dc71e06f9dade75a9b84e9524071dbbc319425' THEN 'TokenRedeemAndSwap'
            WHEN topic_0 = '0x79c15604b92ef54d3f61f0c40caab8857927ca3d5092367163b4562c1699eb5f' THEN 'TokenDepositAndSwap'
        END AS event_name,
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) AS to_address,
        segmented_data,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [0] :: STRING)) AS chainId,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS token,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [2] :: STRING)) AS amount,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [3] :: STRING)) AS tokenIndexFrom,
        -- source chain token index
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [4] :: STRING)) AS tokenIndexTo,
        -- dst chain token index
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [5] :: STRING)) AS minDy,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [6] :: STRING)) AS deadline,
        -- timestamp
        tx_succeeded,
        _log_id,
        modified_timestamp
    FROM
        base_evt
    WHERE
        topic_0 IN (
            '0x91f25e9be0134ec851830e0e76dc71e06f9dade75a9b84e9524071dbbc319425',
            -- TokenRedeemAndSwap
            '0x79c15604b92ef54d3f61f0c40caab8857927ca3d5092367163b4562c1699eb5f' -- TokenDepositAndSwap
        )
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
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) AS to_address,
        segmented_data,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [0] :: STRING)) AS chainId,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS token,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [2] :: STRING)) AS amount,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [3] :: STRING)) AS swapTokenIndex,
        -- dst chain token index
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [4] :: STRING)) AS swapMinAmount,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [5] :: STRING)) AS swapDeadline,
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
        CASE
            WHEN topic_0 = '0xdc5bad4651c5fbe9977a696aadc65996c468cde1448dd468ec0d83bf61c4b57c' THEN 'TokenRedeem'
            WHEN topic_0 = '0xda5273705dbef4bf1b902a131c2eac086b7e1476a8ab0cb4da08af1fe1bd8e3b' THEN 'TokenDeposit'
            WHEN topic_0 = '0x8e57e8c5fea426159af69d47eda6c5052c7605c9f70967cf749d4aa55b70b499' THEN 'TokenRedeemV2'
        END AS event_name,
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) AS to_address,
        segmented_data,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [0] :: STRING)) AS chainId,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS token,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [2] :: STRING)) AS amount,
        tx_succeeded,
        _log_id,
        modified_timestamp
    FROM
        base_evt
    WHERE
        topic_0 IN (
            '0xdc5bad4651c5fbe9977a696aadc65996c468cde1448dd468ec0d83bf61c4b57c',
            -- TokenRedeem
            '0xda5273705dbef4bf1b902a131c2eac086b7e1476a8ab0cb4da08af1fe1bd8e3b',
            -- TokenDeposit
            '0x8e57e8c5fea426159af69d47eda6c5052c7605c9f70967cf749d4aa55b70b499' -- TokenRedeemV2 (terra specific)
        )
),
all_evts AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        contract_address,
        event_index,
        event_name,
        to_address,
        segmented_data,
        chainId,
        token,
        amount,
        tx_succeeded,
        _log_id,
        modified_timestamp
    FROM
        redeem_swap
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        contract_address,
        event_index,
        event_name,
        to_address,
        segmented_data,
        chainId,
        token,
        amount,
        tx_succeeded,
        _log_id,
        modified_timestamp
    FROM
        redeem_remove
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        contract_address,
        event_index,
        event_name,
        to_address,
        segmented_data,
        chainId,
        token,
        amount,
        tx_succeeded,
        _log_id,
        modified_timestamp
    FROM
        redeem_only
)
SELECT
    block_number,
    block_timestamp,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_hash,
    event_index,
    event_name,
    contract_address AS bridge_address,
    origin_from_address AS sender,
    to_address AS receiver,
    receiver AS destination_chain_receiver,
    chainId AS destination_chain_id,
    token AS token_address,
    amount,
    tx_succeeded,
    'synapse' AS platform,
    _log_id,
    modified_timestamp
FROM
    all_evts
