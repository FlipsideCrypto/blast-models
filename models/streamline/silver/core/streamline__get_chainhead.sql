{{ config (
    materialized = 'table',
    tags = ['streamline_core_complete']
) }}

SELECT
    live.udf_api(
        'POST',
        '{service}{Authentication}',{},{ 'method' :'eth_blockNumber',
        'params' :[],
        'id' :1,
        'jsonrpc' :'2.0' },
        'vault/dev/blast/node/mainnet' --update for prod
    ) AS resp,
    utils.udf_hex_to_int(
        resp :data :result :: STRING
    ) AS block_number
