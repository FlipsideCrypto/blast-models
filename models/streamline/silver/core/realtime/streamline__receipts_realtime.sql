{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_rest_api(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'receipts_testnet', 'exploded_key','[\"result\"]', 'sql_limit', {{var('sql_limit','100000')}}, 'producer_batch_size', {{var('producer_batch_size','100000')}}, 'worker_batch_size', {{var('worker_batch_size','50000')}}, 'sm_secret_name','dev/blast/node/testnet'))",
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
to_do AS (
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
        AND block_number IS NOT NULL
    EXCEPT
    SELECT
        block_number
    FROM
        {{ ref("streamline__complete_receipts") }}
    WHERE
        block_number >= (
            SELECT
                block_number
            FROM
                last_3_days
        )
        AND _inserted_timestamp >= DATEADD(
            'day',
            -4,
            SYSDATE()
        )
),
ready_blocks AS (
    SELECT
        block_number
    FROM
        to_do
    UNION
    SELECT
        block_number
    FROM
        (
            SELECT
                block_number
            FROM
                {{ ref("_missing_receipts") }}
            UNION
            SELECT
                block_number
            FROM
                {{ ref("_missing_txs") }}
            UNION
            SELECT
                block_number
            FROM
                {{ ref("_unconfirmed_blocks") }}
        )
)
SELECT
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
                'eth_getBlockReceipts',
                'params',
                ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number)),
                'id',
                '1',
                'jsonrpc',
                '2.0')
            )
        ) AS request
        FROM
            ready_blocks
        ORDER BY
            block_number ASC
        LIMIT
            1000
