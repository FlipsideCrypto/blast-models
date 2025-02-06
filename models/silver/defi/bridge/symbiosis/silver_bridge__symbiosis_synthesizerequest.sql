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
        'symbiosis' AS NAME,
        event_index,
        topics [0] :: STRING AS topic_0,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        'SynthesizeRequest' AS event_name,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [2] :: STRING
            )
        ) AS amount,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                topics [2] :: STRING
            )
        ) AS chainID,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS from_address,
        segmented_data [0] :: STRING AS id,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS revertableAddress,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS to_address,
        CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 25, 40)) AS token,
        tx_succeeded,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] :: STRING = '0x31325fe0a1a2e6a5b1e41572156ba5b4e94f0fae7e7f63ec21e9b5ce1e4b3eab'
        AND contract_address = '0x5aa5f7f84ed0e5db0a4a85c3947ea16b53352fd4'
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
    topic_0,
    event_name,
    tx_succeeded,
    contract_address AS bridge_address,
    NAME AS platform,
    from_address AS sender,
    to_address AS receiver,
    receiver AS destination_chain_receiver,
    amount,
    chainID AS destination_chain_id,
    id,
    revertableAddress AS revertable_address,
    token AS token_address,
    _log_id,
    modified_timestamp
FROM
    base_evt
