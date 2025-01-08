{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

WITH blast_contracts AS (

    SELECT
        *
    FROM
        {{ ref('silver__contracts') }}
    WHERE
        token_name LIKE 'INIT%'
    {% if is_incremental() %}
        AND _inserted_timestamp > (
            SELECT
                max(_inserted_timestamp)
            FROM
                {{ this }}
        )
        AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
    {% endif %}
),
underlying AS (
    SELECT
        tx_hash,
        block_timestamp,
        block_number,
        from_address AS token_address,
        to_address AS underlying_asset_address
    FROM
        {{ ref('core__fact_traces') }}
    WHERE
        identifier = 'CALL_0_1'
        AND LEFT(
            input,
            10
        ) = '0x095ea7b3'
        AND trace_status = 'SUCCESS'
        AND from_address IN (
            SELECT
                contract_address
            FROM
                blast_contracts
        )
),
unwrapped AS (
    SELECT
        from_address AS underlying_asset_address,
        to_address AS underlying_unwrap_address
    FROM
        {{ ref('core__fact_traces') }}
    WHERE
        identifier = 'CALL_0_0'
        AND LEFT(
            input,
            10
        ) = '0x1a33757d'
        AND trace_status = 'SUCCESS'
        AND from_address IN (
            SELECT
                underlying_asset_address
            FROM
                underlying
        )
)
SELECT
    A.block_timestamp,
    A.block_number,
    A.tx_hash,
    token_address,
    b.token_name,
    b.token_symbol,
    b.token_decimals,
    underlying_asset_address,
    C.token_name AS underlying_name,
    C.token_symbol AS underlying_symbol,
    C.token_decimals AS underlying_decimals,
    d.underlying_unwrap_address,
    e.token_name AS underlying_unwrap_name,
    e.token_symbol AS underlying_unwrap_symbol,
    e.token_decimals AS underlying_unwrap_decimals,
    b._inserted_timestamp
FROM
    underlying A
    INNER JOIN blast_contracts b
    ON b.contract_address = token_address
    LEFT JOIN unwrapped d USING(underlying_asset_address)
    INNER JOIN contracts C
    ON C.contract_address = underlying_asset_address
    LEFT JOIN contracts e
    ON e.contract_address = underlying_unwrap_address