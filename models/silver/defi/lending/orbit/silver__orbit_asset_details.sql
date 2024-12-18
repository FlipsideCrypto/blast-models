{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

WITH contracts AS (
    SELECT
        *
    FROM
        {{ ref('silver__contracts') }}
),
log_pull AS (
    SELECT
        l.tx_hash,
        l.block_number,
        l.block_timestamp,
        l.contract_address,
        C.token_name,
        C.token_symbol,
        C.token_decimals,
        l.modified_timestamp,
        l._log_id
    FROM
        {{ ref('core__fact_event_logs') }} l
        LEFT JOIN contracts C
        ON C.contract_address = l.contract_address
    WHERE
        topics [0] :: STRING = '0x7ac369dbd14fa5ea3f473ed67cc9d598964a77501540ba6751eb0b3decf5870d'
        AND origin_from_address='0x6315f65843e7582508e4f0aac20a7203e7b09f02'
    {% if is_incremental() %}
        AND l.modified_timestamp > (
            SELECT
                max(modified_timestamp)
            FROM
                {{ this }}
        )
        AND l.modified_timestamp >= SYSDATE() - INTERVAL '7 day'
    {% endif %}
),
traces_pull AS (
    SELECT
        t.from_address AS token_address,
        t.to_address AS underlying_asset
    FROM
        {{ ref('core__fact_traces') }} t
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                log_pull
        )
        AND input = '0x18160ddd' 
        AND type = 'STATICCALL'
    {% if is_incremental() %}
        AND t.modified_timestamp > (
            SELECT
                max(modified_timestamp)
            FROM
                {{ this }}
        )
        AND t.modified_timestamp >= SYSDATE() - INTERVAL '7 day'
    {% endif %}
),
underlying_details AS (
    SELECT
        l.tx_hash,
        l.block_number,
        l.block_timestamp,
        l.contract_address,
        l.token_name,
        l.token_symbol,
        l.token_decimals,
        CASE WHEN t.underlying_asset IS NULL THEN '0x4300000000000000000000000000000000000004'
        ELSE t.underlying_asset
        END AS underlying_asset,
        l.modified_timestamp,
        l._log_id
    FROM
        log_pull l
        LEFT JOIN traces_pull t
        ON l.contract_address = t.token_address
    QUALIFY(ROW_NUMBER() OVER(PARTITION BY l.contract_address
    ORDER BY
        block_timestamp ASC)) = 1
)
SELECT
    l.tx_hash,
    l.block_number,
    l.block_timestamp,
    l.contract_address AS token_address,
    l.token_name,
    l.token_symbol,
    l.token_decimals,
    l.underlying_asset AS underlying_asset_address,
    C.token_name AS underlying_name,
    C.token_symbol AS underlying_symbol,
    C.token_decimals AS underlying_decimals,
    l.modified_timestamp,
    l._log_id
FROM
    underlying_details l
    LEFT JOIN contracts C
    ON C.contract_address = l.underlying_asset
WHERE
    l.token_name IS NOT NULL