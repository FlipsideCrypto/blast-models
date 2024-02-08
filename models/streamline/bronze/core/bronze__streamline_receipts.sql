{{ config (
    materialized = 'view'
) }}

WITH num_seq AS (

    SELECT
        _id AS block_number
    FROM
        {{ ref('silver__number_sequence') }}
    WHERE
        _id > 1300000
        AND _id <= 1300010
),
bronze AS (
    SELECT
        block_number,
        utils.udf_int_to_hex(block_number) AS block_hex,
        live.udf_api(
            'POST',
            '{blast_testnet_url}',{},{ 'method' :'eth_getBlockReceipts',
            'params' :[ block_hex ],
            'id' :1,
            'jsonrpc' :'2.0' },
            'quicknode_blast_testnet'
        ) AS resp,
        resp :data :result AS resp_data,
        SYSDATE() AS _inserted_timestamp
    FROM
        num_seq
)
SELECT
    block_number,
    resp,
    resp_data,
    VALUE AS DATA,
    _inserted_timestamp
FROM
    bronze,
    LATERAL FLATTEN (
        input => resp_data
    )
