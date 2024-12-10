{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH hyperlane_assets AS (

    SELECT
        DISTINCT contract_address
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] :: STRING IN (
            '0xd229aacb94204188fe8042965fa6b269c62dc5818b21238779ab64bdd17efeec',
            -- SentTransferRemote
            '0xba20947a325f450d232530e5f5fce293e7963499d5309a07cee84a269f2f15a6' -- ReceivedTransferRemote
        )

{% if is_incremental() %}
AND contract_address NOT IN (
    SELECT
        DISTINCT contract_address
    FROM
        {{ this }}
)
{% endif %}
),
dispatch AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        event_index,
        contract_address,
        event_removed,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS sender,
        -- src bridge token address, not user address
        TRY_TO_NUMBER(utils.udf_hex_to_int(topics [2] :: STRING)) AS destination,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS recipient,
        -- dst bridge token address, not recipient address
        DATA,
        CASE
            WHEN tx_status = 'success' THEN TRUE
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
        topics [0] :: STRING = '0x769f711d20c679153d382254f59892613b58a97cc876b249134ac25c80f9c814'
        AND contract_address = LOWER('0x3a867fCfFeC2B790970eeBDC9023E75B0a172aa7')
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
dispatch_id AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        topics [1] :: STRING AS messageId,
        CASE
            WHEN tx_status = 'success' THEN TRUE
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
        topics [0] :: STRING = '0x788dbc1b7152732178210e7f4d9d010ef016f9eafbe66786bd7169f56e0c353a'
        AND contract_address = LOWER('0x3a867fCfFeC2B790970eeBDC9023E75B0a172aa7')
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
gas_payment AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        topics [1] :: STRING AS messageId,
        TRY_TO_NUMBER(utils.udf_hex_to_int(topics [2] :: STRING)) AS destinationDomain,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [0] :: STRING)) AS gasAmount,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [1] :: STRING)) AS payment,
        CASE
            WHEN tx_status = 'success' THEN TRUE
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
        topics [0] = '0x65695c3748edae85a24cc2c60b299b31f463050bc259150d2e5802ec8d11720a'
        AND contract_address = ('0xB3fCcD379ad66CED0c91028520C64226611A48c9')
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
sent_transfer_remote AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS destination,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS receipient,
        -- actual receipient
        TRY_TO_NUMBER(utils.udf_hex_to_int(DATA :: STRING)) AS amount,
        CASE
            WHEN tx_status = 'success' THEN TRUE
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
        topics [0] :: STRING = '0xd229aacb94204188fe8042965fa6b269c62dc5818b21238779ab64bdd17efeec'
        AND contract_address IN (
            SELECT
                *
            FROM
                hyperlane_assets
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
-- this can be replaced by a 1 contract read of contracts in the hyperlane_asset (hyperlane_asset contracts have a wrappedtoken function)
token_transfer AS (
    -- this matches tx_hash with each token's burn tx. this works since each contract only handles 1 token.
    SELECT
        tx_hash,
        contract_address AS token_address
    FROM
        {{ ref('silver__transfers') }}
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                sent_transfer_remote
        )
        AND to_address = '0x0000000000000000000000000000000000000000'
)
SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    contract_address,
    'Dispatch' AS event_name,
    event_removed,
    tx_succeeded,
    -- from dispatch
    sender,
    -- src bridge token
    receipient AS destination_chain_receiver,
    -- dst bridge token
    destination AS destinationChainId,
    -- from dispatch_id
    messageId,
    -- from gas_payment
    gasAmount,
    payment,
    -- from sent_transfer_remote
    receipient AS receiver,
    -- actual receiver address
    amount -- from token_transfer
    token_address,
    _log_id,
    modified_timestamp
FROM
    dispatch
    INNER JOIN dispatch_id USING(tx_hash)
    INNER JOIN gas_payment USING(tx_hash)
    INNER JOIN token_transfer USING(tx_hash)
