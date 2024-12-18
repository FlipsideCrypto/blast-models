{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

WITH asset_list AS (
    SELECT
        '0x44f33bc796f7d3df55040cd3c631628b560715c2' AS pool_address
    UNION ALL
    SELECT
        '0x4a1d9220e11a47d8ab22ccd82da616740cf0920a' AS pool_address
    UNION ALL
    SELECT
        '0x788654040d7e9a8bb583d7d8ccea1ebf1ae4ac06' AS pool_address
    UNION ALL
    SELECT
        '0x60ed5493b35f833189406dfec0b631a6b5b57f66' AS pool_address
),
contracts AS (
    SELECT
        *
    FROM
        {{ ref('core__dim_contracts') }}
),
juice_contracts AS (
    SELECT
        *
    FROM
        contracts
    WHERE
        NAME LIKE 'Juice%Collateral%'
),
collateral_tokens AS (
    SELECT
        '0x1d37383447ceceeedb7c92372d6993821d3d7b40' AS contract_address,
        '0x211cc4dd073734da055fbf44a2b4667d5e5fe5d2' AS underlying_asset
    UNION ALL
    SELECT
        '0x7e4afebe294345d72de6bb8405c871d7bb6c53d1' AS contract_address,
        '0x04c0599ae5a44757c0af6f9ec3b93da8976c150a' AS underlying_asset
    UNION ALL
    SELECT
        '0x295e17672f1290b66dd064ec6b7fdaf280b33cea' AS contract_address,
        '0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34' AS underlying_asset
    UNION ALL
    SELECT
        '0x0246937acacabe4e1b6045de9b68113d72966be2' AS contract_address,
        '0xb1a5700fa2358173fe465e6ea4ff52e36e88e2ad' AS underlying_asset
    UNION ALL
    SELECT
        '0x2b1c36a733b1bab31f05ac8866d330e29c604b8f' AS contract_address,
        '0xb1a5700fa2358173fe465e6ea4ff52e36e88e2ad' AS underlying_asset
    UNION ALL
    SELECT
        '0xc81a630806d1af3fd7509187e1afc501fd46e818' AS contract_address,
        '0x2416092f143378750bb29b79ed961ab195cceea5' AS underlying_asset
),
tx_pull AS (
    SELECT
        *
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        origin_from_address = '0x0ee09b204ffebf9a1f14c99e242830a09958ba34'
        AND origin_to_address = '0x4e59b44847b379578588920ca78fbf26c0b4956c'
        AND CONCAT('0x', SUBSTR(topics [1], 27, 40)) IN (
            SELECT
                pool_address
            FROM
                asset_list
        )
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
trace_pull AS (
    SELECT
        *
    FROM
        {{ ref('core__fact_traces') }}
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                tx_pull
        )
        AND identifier IN (
            'CREATE_0_5',
            'CREATE_0_4'
        )
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
debt_token AS (
    SELECT
        tx_hash,
        block_timestamp,
        block_number,
        from_address AS pool_address,
        to_address AS debt_address,
        NAME AS debt_name,
        decimals AS debt_decimals,
        symbol AS debt_symbol,
        l.modified_timestamp
    FROM
        trace_pull l
        LEFT JOIN contracts
        ON address = debt_address
    WHERE
        identifier = 'CREATE_0_4'
),
token AS (
    SELECT
        tx_hash,
        block_timestamp,
        to_address AS token_address,
        NAME AS token_name,
        decimals AS token_decimals,
        symbol AS token_symbol,
        a.modified_timestamp
    FROM
        trace_pull a
        LEFT JOIN contracts
        ON address = token_address
    WHERE
        identifier = 'CREATE_0_5'
),
underlying AS (
    SELECT
        *,
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                tx_pull
        )
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
underlying_asset AS (
    SELECT
        tx_hash,
        t2.contract_address AS underlying_asset_address,
        CONCAT('0x', SUBSTR(topics [1], 27, 40)) AS contract,
        NAME AS underlying_name,
        decimals AS underlying_decimals,
        symbol AS underlying_symbol,
        t1.modified_timestamp,
        t1._log_id
    FROM
        underlying t1
        INNER JOIN underlying t2 USING(tx_hash)
        LEFT JOIN contracts t5
        ON underlying_asset_address = t5.address
    WHERE
        topics [1] IS NOT NULL
        AND t2.contract_address != '0x2536fe9ab3f511540f2f9e2ec2a805005c3dd800'
),
logs_pull AS (
    SELECT
        *,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(segmented_data [0], 25, 40)) AS contract_address,
        modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address = LOWER('0x2536FE9ab3F511540F2f9e2eC2A805005C3Dd800')
        AND topics [0] = '0x2da9afcf2ffbfd720263cc579aa9f8dfce34b31d447b0ba6d0bfefc40f713c84'
        AND CONCAT('0x', SUBSTR(segmented_data [0], 25, 40)) IN (
            SELECT
                address
            FROM
                juice_contracts
        )
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
get_underlying AS (
    SELECT
        *
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                logs_pull
        )
        AND topics [0] = '0xcaa97ab28bae75adcb5a02786c64b44d0d3139aa521bf831cdfbe280ef246e36'
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
collateral_base AS (
    SELECT
        tx_hash,
        block_timestamp,
        block_number,
        t1.contract_address,
        t2.contract_address AS underlying_asset,
        t1.modified_timestamp,
        _log_id
    FROM
        logs_pull t1
        LEFT JOIN get_underlying t2 USING(tx_hash)
),
collateral_list AS (
    SELECT
        tx_hash,
        block_timestamp,
        block_number,
        C.address AS token_address,
        C.name AS token_name,
        C.symbol AS token_symbol,
        C.decimals AS token_decimals,
        COALESCE(
            A.underlying_asset,
            b.underlying_asset
        ) AS underlying_asset_address,
        d.name AS underlying_name,
        d.symbol AS underlying_symbol,
        d.decimals AS underlying_decimals,
        A.modified_timestamp,
        _log_id
    FROM
        collateral_base A
        LEFT JOIN collateral_tokens b USING(contract_address)
        LEFT JOIN contracts C
        ON A.contract_address = C.address
        LEFT JOIN contracts d
        ON underlying_asset_address = d.address
),
combine_asset AS (
    SELECT
        block_timestamp,
        block_number,
        tx_hash,
        underlying_asset_address,
        underlying_name,
        underlying_decimals,
        underlying_symbol,
        pool_address,
        token_address,
        token_name,
        token_decimals,
        token_symbol,
        debt_address,
        debt_name,
        debt_decimals,
        debt_symbol,
        modified_timestamp,
        _log_id
    FROM
        underlying_asset
        INNER JOIN token t3 USING(tx_hash)
        INNER JOIN debt_token t4 USING(tx_hash)
    UNION ALL
    SELECT
        block_timestamp,
        block_number,
        tx_hash,
        underlying_asset_address,
        underlying_name,
        underlying_decimals,
        underlying_symbol,
        NULL AS pool_address,
        token_address,
        token_name,
        token_decimals,
        token_symbol,
        NULL AS debt_address,
        NULL AS debt_name,
        NULL AS debt_decimals,
        NULL AS debt_symbol,
        modified_timestamp,
        _log_id
    FROM
        collateral_list
)
SELECT
    *
FROM
    combine_asset
