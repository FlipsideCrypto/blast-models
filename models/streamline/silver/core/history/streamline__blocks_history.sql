{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_rest_api(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'blocks_testnet', 'sql_limit', {{var('sql_limit','100000')}}, 'producer_batch_size', {{var('producer_batch_size','100000')}}, 'worker_batch_size', {{var('worker_batch_size','50000')}}, 'sm_secret_name','dev/blast/node/testnet'))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_core_history']
) }}

WITH last_3_days AS (

    SELECT
        block_number
    FROM
        {{ ref("_block_lookback") }}
),
blocks AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(
            ['block_number']
        ) }} AS id,
        block_number
    FROM
        {{ ref("streamline__blocks") }}
    WHERE
        block_number <= (
            SELECT
                block_number
            FROM
                last_3_days
        )
    EXCEPT
    SELECT
        id,
        block_number
    FROM
        {{ ref("streamline__complete_blocks") }}
    WHERE
        block_number <= (
            SELECT
                block_number
            FROM
                last_3_days
        )
)
SELECT
    id,
    block_number,
    ARRAY_CONSTRUCT(
        block_number,
        ARRAY_CONSTRUCT(
            'POST',
            '{service}/{Authentication}/',
            PARSE_JSON('{}'),
            PARSE_JSON('{}'),
            OBJECT_CONSTRUCT(
                'method',
                'eth_getBlockByNumber',
                'params',
                ARRAY_CONSTRUCT(
                    utils.udf_int_to_hex(block_number),
                    FALSE
                ),
                'id',
                '1',
                'jsonrpc',
                '2.0'
            )
        )
    ) AS request
FROM
    blocks
ORDER BY
    block_number ASC
