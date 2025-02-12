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
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        'across-v3' AS NAME,
        topics,
        DATA,
        event_index,
        event_name,
        utils.udf_hex_to_int(
            topics [1] :: STRING
        ) AS destinationChainId,
        utils.udf_hex_to_int(
            topics [2] :: STRING
        ) AS depositId,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS depositor,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS inputToken,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS outputToken,
        TRY_TO_NUMBER(utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        )) AS inputAmount,
        TRY_TO_NUMBER(utils.udf_hex_to_int(
            segmented_data [3] :: STRING
        )) AS outputAmount,
        utils.udf_hex_to_int(
            segmented_data [4] :: STRING
        ) AS quotedTimestamp,
        utils.udf_hex_to_int(
            segmented_data [5] :: STRING
        ) AS fillDeadline,
        utils.udf_hex_to_int(
            segmented_data [6] :: STRING
        ) AS exclusivityDeadline,
        CONCAT('0x', SUBSTR(segmented_data [7] :: STRING, 25, 40)) AS recipient,
        CONCAT('0x', SUBSTR(segmented_data [8] :: STRING, 25, 40)) AS exclusiveRelayer,
        segmented_data [9] :: STRING AS message,
        tx_succeeded,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        topics [0] :: STRING = '0xa123dc29aebf7d0c3322c8eeb5b999e859f39937950ed31056532713d0de396f'
        AND contract_address = '0x2d509190ed0172ba588407d4c2df918f955cc6e1'
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
)
SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    topics,
    event_name,
    tx_succeeded,
    contract_address AS bridge_address,
    NAME AS platform,
    depositor AS sender,
    recipient AS receiver,
    recipient AS destination_chain_receiver,
    destinationChainId AS destination_chain_id,
    inputAmount AS amount,
    inputToken AS token_address,
    depositId AS deposit_id,
    message,
    exclusiveRelayer AS exclusive_relayer,
    exclusivityDeadline AS exclusivity_deadline,
    fillDeadline AS fill_deadline,
    outputAmount AS output_amount,
    outputToken AS output_token,
    _log_id,
    modified_timestamp
FROM
    base_evt
