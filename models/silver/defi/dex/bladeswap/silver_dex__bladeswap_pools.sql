{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'pool_address',
    tags = ['curated']
) }}

WITH created_pools AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS pool_address,
        CASE
            WHEN segmented_data [0] :: STRING = '000000000000000000000000eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0x4300000000000000000000000000000000000004'
            ELSE CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40))
        END AS token0,
        CASE
            WHEN segmented_data [1] :: STRING = '000000000000000000000000eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0x4300000000000000000000000000000000000004'
            ELSE CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40))
        END AS token1,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref ('silver__logs') }}
    WHERE
        contract_address = LOWER('0x75cb3ec310d3d1e22637f79d61eab5d9abcd68bd') --XYKPoolFactory
        AND topics [0] :: STRING = '0x5f28326c23d3e7607ba7f55dcee451cc4dbb16e3f016b27f013c99fb1d7d4168' --PairCreated
        AND tx_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    contract_address,
    token0,
    token1,
    pool_address,
    _log_id,
    _inserted_timestamp
FROM
    created_pools qualify(ROW_NUMBER() over (PARTITION BY pool_address
ORDER BY
    _inserted_timestamp DESC)) = 1
