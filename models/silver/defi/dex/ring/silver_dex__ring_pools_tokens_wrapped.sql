{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    tags = ['curated']
) }}

SELECT
    block_number,
    tx_hash,
    event_index,
    contract_address,
    regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
    CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS original_token,
    CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS wrapped_token,
    utils.udf_hex_to_int(
        segmented_data [1] :: STRING
    ) :: INTEGER AS asset_id,
    CONCAT(
        tx_hash,
        '-',
        event_index
    ) AS _log_id,
    modified_timestamp AS _inserted_timestamp
FROM
    {{ ref('core__fact_event_logs') }}
WHERE
    contract_address = '0x455b20131d59f01d082df1225154fda813e8cee9' --FewFactory
    AND topics [0] :: STRING = '0x940398321949af993516f7d144a2b9f43100b1de59365ba376e2b458d840c091' --WrappedTokenCreated
    AND tx_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
