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
            '{blast_testnet_url}',{},{ 'method' :'debug_traceBlockByNumber',
            'params' :[ block_hex, {'tracer': 'callTracer','timeout': '30s'} ],
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
    VALUE :result AS full_traces,
    INDEX :: INT AS tx_position,
    _inserted_timestamp
FROM
    bronze,
    LATERAL FLATTEN (
        input => resp_data
    )
