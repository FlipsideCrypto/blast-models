{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH bridge_native AS (
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
        origin_from_address AS depositor,
        RIGHT(
            amount_precise_raw,
            4
        ) AS destinationChainId,
        amount_precise_raw :: INTEGER AS amount_unadj,
        origin_from_address AS recipient,
        et.to_address AS bridge_address,
        trace_index,
        native_transfers_id,
        et.modified_timestamp
    FROM
        {{ ref('silver__native_transfers') }}
        et
        INNER JOIN {{ ref('core__fact_transactions') }}
        tx
        ON et.block_number = tx.block_number
        AND et.tx_hash = tx.tx_hash
    WHERE
        et.to_address IN (
            '0xe4edb277e41dc89ab076a1f049f4a3efa700bce8',
            '0xee73323912a4e3772b74ed0ca1595a152b0ef282',
            '0x80c67432656d59144ceff962e8faf8926599bcf8',
            '0x3bdb03ad7363152dfbc185ee23ebc93f0cf93fd1'
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
)
SELECT
    block_number,
    block_timestamp,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_hash,
    trace_index AS event_index,
    NULL AS event_name,
    to_address,
    from_address,
    depositor AS sender,
    b.destinationChainId AS orbiter_chain_id,
    s.chainid AS destination_chain_id,
    s.name AS destination_chain,
    amount_unadj,
    recipient AS receiver,
    recipient AS destination_chain_receiver,
    bridge_address,
    TRUE AS tx_succeeded,
    'orbiter' AS platform,
    '0x4300000000000000000000000000000000000004' AS token_address,
    --weth contract address
    native_transfers_id AS _id,
    modified_timestamp
FROM
    bridge_native b
    LEFT JOIN {{ ref('silver_bridge__orbiter_bridge_seed') }}
    s
    ON b.destinationChainId :: STRING = s.identificationcode :: STRING
