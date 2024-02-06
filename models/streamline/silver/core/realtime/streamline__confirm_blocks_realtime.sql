{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_json_rpc(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'confirm_blocks', 'sql_limit', {{var('sql_limit','100000')}}, 'producer_batch_size', {{var('producer_batch_size','100000')}}, 'worker_batch_size', {{var('worker_batch_size','100000')}}, 'batch_call_limit', {{var('batch_call_limit','3')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_core_realtime']
) }}

WITH last_3_days AS (

    SELECT
        block_number
    FROM
        {{ ref("_block_lookback") }}
),
last_3_days AS (
    SELECT
        block_number
    FROM
        {{ ref("_max_block_by_date") }}
        qualify ROW_NUMBER() over (
            ORDER BY
                block_number DESC
        ) = 3
),
tbl AS (
    SELECT
        block_number
    FROM
        {{ ref("streamline__blocks") }}
    WHERE
        (
            block_number >= (
                SELECT
                    block_number
                FROM
                    last_3_days
            )
        )
        AND block_number >= (
            SELECT
                block_number
            FROM
                last_3_days
        )
    EXCEPT
    SELECT
        block_number
    FROM
        {{ ref("streamline__complete_confirmed_blocks") }}
    WHERE
        (
            block_number >= (
                SELECT
                    block_number
                FROM
                    last_3_days
            )
        )
        AND _inserted_timestamp >= DATEADD(
            'day',
            -4,
            SYSDATE()
        )
        AND block_number >= (
            SELECT
                block_number
            FROM
                last_3_days
        )
)
SELECT
    PARSE_JSON(
        CONCAT(
            '{"jsonrpc": "2.0",',
            '"method": "eth_getBlockByNumber", "params":["',
            REPLACE(
                concat_ws(
                    '',
                    '0x',
                    to_char(
                        block_number :: INTEGER,
                        'XXXXXXXX'
                    )
                ),
                ' ',
                ''
            ),
            '", false],"id":"',
            block_number :: INTEGER,
            '"}'
        )
    ) AS request
FROM
    tbl
ORDER BY
    block_number ASC
LIMIT
    20000
