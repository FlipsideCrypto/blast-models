{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

WITH blast_contracts AS (

    SELECT
        address,
        symbol,
        NAME,
        decimals,
        modified_timestamp
    FROM
        {{ ref('core__dim_contracts') }}
    WHERE
        NAME LIKE 'INIT%'
    {% if is_incremental() %}
        AND modified_timestamp > (
            SELECT
                max(modified_timestamp)
            FROM
                {{ this }}
        )
        AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
    {% endif %}
),
contracts AS (

    SELECT
        address,
        symbol,
        NAME,
        decimals,
        modified_timestamp
    FROM
        {{ ref('core__dim_contracts') }}
),
underlying AS (
    SELECT
        tx_hash,
        block_timestamp,
        block_number,
        CASE
            WHEN trace_status = 'SUCCESS' THEN TRUE
            ELSE FALSE
        END AS trace_succeeded,
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
        AND trace_succeeded
        AND from_address IN (
            SELECT
                address
            FROM
                blast_contracts
        )
),
unwrapped AS (
    SELECT
        from_address AS underlying_asset_address,
        to_address AS underlying_unwrap_address,
        CASE
            WHEN trace_status = 'SUCCESS' THEN TRUE
            ELSE FALSE
        END AS trace_succeeded
    FROM
        {{ ref('core__fact_traces') }}
    WHERE
        identifier = 'CALL_0_0'
        AND LEFT(
            input,
            10
        ) = '0x1a33757d'
        AND trace_succeeded
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
    b.name AS token_name,
    b.symbol AS token_symbol,
    b.decimals AS token_decimals,
    underlying_asset_address,
    C.name AS underlying_name,
    C.symbol AS underlying_symbol,
    C.decimals AS underlying_decimals,
    d.underlying_unwrap_address,
    e.name AS underlying_unwrap_name,
    e.symbol AS underlying_unwrap_symbol,
    e.decimals AS underlying_unwrap_decimals,
    b.modified_timestamp
FROM
    underlying A
    INNER JOIN blast_contracts b
    ON b.address = token_address
    LEFT JOIN unwrapped d USING(underlying_asset_address)
    INNER JOIN contracts C
    ON C.address = underlying_asset_address
    LEFT JOIN contracts e
    ON e.address = underlying_unwrap_address