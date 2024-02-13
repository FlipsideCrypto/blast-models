{{ config (
    materialized = "view",
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
        ) AS block_number
)
SELECT
    _id AS block_number,
    REPLACE(
        concat_ws('', '0x', to_char(block_number, 'XXXXXXXX')),
        ' ',
        ''
    ) AS block_number_hex
FROM
    {{ ref("silver__number_sequence") }}
WHERE
    _id <= (
        SELECT
            COALESCE(
                block_number,
                0
            )
        FROM
            get_chainhead
    )
ORDER BY
    _id ASC
