{{ config (
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number'],
    tags = ['streamline_core_complete']
) }}

WITH get_chainhead AS (

    SELECT
        live.udf_api(
            'POST',
            '{blast_testnet_url}',
            --update for prod
            {},{ 'method' :'eth_blockNumber',
            'params' :[],
            'id' :1,
            'jsonrpc' :'2.0' },
            'quicknode_blast_testnet' --update for prod
        ) AS resp,
        utils.udf_hex_to_int(
            resp :data :result :: STRING
        ) AS block_number,
        SYSDATE() AS _inserted_timestamp

{% if is_incremental() %}
WHERE
    block_number NOT IN (
        SELECT
            DISTINCT block_number
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    block_number
FROM
    get_chainhead
ORDER BY block_number DESC
LIMIT 1
