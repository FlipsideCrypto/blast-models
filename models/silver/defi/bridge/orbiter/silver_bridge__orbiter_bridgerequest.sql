{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH bridge_router AS (
    -- for bridge tx utilizing router

    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        contract_address,
        event_index,
        'bridgeRequest' AS event_name,
        origin_to_address AS to_address,
        origin_from_address AS from_address,
        origin_from_address AS depositor,
        RIGHT(utils.udf_hex_to_int(DATA :: STRING), 4) AS destinationChainId,
        TRY_TO_NUMBER(utils.udf_hex_to_int(DATA :: STRING)) AS amount,
        origin_from_address AS recipient,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS bridge_address,
        CASE
            WHEN tx_status = 'SUCCESS' THEN TRUE
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
        topics [0] :: STRING = '0x69ca02dd4edd7bf0a4abb9ed3b7af3f14778db5d61921c7dc7cd545266326de2'
        AND contract_address = '0x13e46b2a3f8512ed4682a8fb8b560589fe3c2172'
        AND bridge_address IN (
            '0xe4edb277e41dc89ab076a1f049f4a3efa700bce8',
            '0xee73323912a4e3772b74ed0ca1595a152b0ef282'
        ) -- orbiter bridge
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
bridge_native AS (
    -- for direct native eth transfers
    SELECT
        et.block_number,
        et.block_timestamp,
        et.tx_hash,
        tx.from_address AS origin_from_address,
        tx.to_address AS origin_to_address,
        tx.origin_function_signature,
        et.to_address,
        et.from_address,
        et.from_address AS depositor,
        RIGHT(
            amount_precise_raw,
            4
        ) AS destinationChainId,
        amount_precise_raw,
        et.from_address AS recipient,
        et.to_address AS bridge_address,
        native_transfers_id,
        et.modified_timestamp
    FROM
        {{ ref('silver__native_transfers') }}
        et
        INNER JOIN {{ ref('silver__transactions') }}
        tx
        ON et.block_number = tx.block_number
        AND et.tx_hash = tx.tx_hash
    WHERE
        et.to_address IN (
            '0xe4edb277e41dc89ab076a1f049f4a3efa700bce8',
            '0xee73323912a4e3772b74ed0ca1595a152b0ef282',
            '0x80c67432656d59144ceff962e8faf8926599bcf8'
        )

{% if is_incremental() %}
AND et.modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND et.modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
bridge_combined AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        event_name,
        to_address,
        from_address,
        depositor,
        destinationChainId AS orbiter_chain_id,
        amount AS amount_unadj,
        recipient,
        bridge_address,
        tx_succeeded,
        _log_id AS _id,
        modified_timestamp
    FROM
        bridge_router
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        NULL AS event_index,
        NULL AS event_name,
        to_address,
        from_address,
        depositor,
        destinationChainId AS orbiter_chain_id,
        amount_precise_raw AS amount_unadj,
        recipient,
        bridge_address,
        TRUE AS tx_succeeded,
        native_transfers_id AS _id,
        modified_timestamp
    FROM
        bridge_native
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
    to_address,
    from_address,
    depositor AS sender,
    orbiter_chain_id,
    s.chainid AS destination_chain_id,
    s.name AS destination_chain,
    amount_unadj,
    recipient AS receiver,
    recipient AS destination_chain_receiver,
    bridge_address,
    'orbiter' AS platform,
    '0x4300000000000000000000000000000000000004' AS token_address,
    --weth contract address
    tx_succeeded,
    _id,
    modified_timestamp
FROM
    bridge_combined b
    LEFT JOIN {{ ref('silver_bridge__orbiter_bridge_seed') }}
    s
    ON b.orbiter_chain_id :: STRING = s.identificationcode :: STRING
