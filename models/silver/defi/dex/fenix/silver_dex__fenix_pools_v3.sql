{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'pool_address',
    cluster_by = ['block_timestamp::DATE'],
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
        LOWER(CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40))) AS token0_address,
        LOWER(CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))) AS token1_address,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS pool_address,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] :: STRING = '0x91ccaa7a278130b65168c3a0c8d3bcae84cf5e43704342bd3ec0b59e59c036db'
        AND contract_address = '0x7a44cd060afc1b6f4c80a2b9b37f4473e74e25df' --Fenix Finance : Algebra Factory
        AND tx_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
pool_info AS (
    SELECT
        contract_address,
        topics [0] :: STRING AS topics_0,
        topics [1] :: STRING AS topics_1,
        topics [2] :: STRING AS topics_2,
        topics [3] :: STRING AS topics_3,
        DATA,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] :: STRING IN (
            '0x98636036cb66a9c19a37435efc1e90142190214e8abeb821bdba3f2990dd4c95',
            '0x598b9f043c813aa6be3426ca60d1c65d17256312890be5118dab55b0775ebe2a',
            '0x01413b1d5d4c359e9a0daa7909ecda165f6e8c51fe2ff529d74b22a5a7c02645'
        )
        AND tx_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
initial_info AS (
    SELECT
        contract_address,
        segmented_data,
        utils.udf_hex_to_int('s2c', CONCAT('0x', segmented_data [0] :: STRING)) :: FLOAT AS init_price,
        utils.udf_hex_to_int('s2c', CONCAT('0x', segmented_data [1] :: STRING)) :: FLOAT AS init_tick,
        pow(
            1.0001,
            init_tick
        ) AS init_price_1_0_unadj,
        _log_id,
        _inserted_timestamp
    FROM
        pool_info
    WHERE
        topics_0 = '0x98636036cb66a9c19a37435efc1e90142190214e8abeb821bdba3f2990dd4c95'
),
fee_info AS (
    SELECT
        contract_address,
        segmented_data,
        utils.udf_hex_to_int('s2c', CONCAT('0x', segmented_data [0] :: STRING)) :: FLOAT AS fee,
        _log_id,
        _inserted_timestamp
    FROM
        pool_info
    WHERE
        topics_0 = '0x598b9f043c813aa6be3426ca60d1c65d17256312890be5118dab55b0775ebe2a'
),
tickspacing_info AS (
    SELECT
        contract_address,
        segmented_data,
        utils.udf_hex_to_int('s2c', CONCAT('0x', segmented_data [0] :: STRING)) :: FLOAT AS tick_spacing,
        _log_id,
        _inserted_timestamp
    FROM
        pool_info
    WHERE
        topics_0 = '0x01413b1d5d4c359e9a0daa7909ecda165f6e8c51fe2ff529d74b22a5a7c02645'
),
FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        p.contract_address,
        token0_address,
        token1_address,
        fee :: INTEGER AS fee,
        (
            fee / 10000
        ) :: FLOAT AS fee_percent,
        tick_spacing,
        pool_address,
        COALESCE(
            init_tick,
            0
        ) AS init_tick,
        p._log_id,
        p._inserted_timestamp
    FROM
        created_pools p
        LEFT JOIN initial_info i
        ON p.pool_address = i.contract_address
        LEFT JOIN fee_info f
        ON p.pool_address = f.contract_address
        LEFT JOIN tickspacing_info t
        ON p.pool_address = t.contract_address
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    contract_address,
    token0_address,
    token1_address,
    fee,
    fee_percent,
    tick_spacing,
    pool_address,
    init_tick,
    _log_id,
    _inserted_timestamp
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY pool_address
ORDER BY
    _inserted_timestamp DESC)) = 1
