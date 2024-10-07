{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    tags = ['curated']
) }}

WITH base_contracts AS (

    SELECT
        token_address AS contract_address,
        MAX(block_number) AS block_number,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM
        (
            SELECT
                block_number,
                token0 AS token_address,
                _inserted_timestamp
            FROM
                {{ ref('silver_dex__ring_pools') }}
            UNION
            SELECT
                block_number,
                token1 AS token_address,
                _inserted_timestamp
            FROM
                {{ ref('silver_dex__ring_pools') }}
            UNION
            SELECT
                block_number,
                token0_address AS token_address,
                _inserted_timestamp
            FROM
                {{ ref('silver_dex__ring_pools_v3') }}
            UNION
            SELECT
                block_number,
                token1_address AS token_address,
                _inserted_timestamp
            FROM
                {{ ref('silver_dex__ring_pools_v3') }}
        )

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
    AND contract_address NOT IN (
        SELECT
            DISTINCT contract_address
        FROM
            {{ this }}
    )
{% endif %}
GROUP BY
    1
),
function_sigs AS (
    SELECT
        '0xfc0c546a' AS function_sig,
        'token' AS function_name
),
inputs AS (
    SELECT
        contract_address,
        block_number,
        function_sig,
        function_name,
        0 AS function_input,
        CONCAT(
            function_sig,
            LPAD(
                function_input,
                64,
                0
            )
        ) AS DATA
    FROM
        base_contracts
        JOIN function_sigs
        ON 1 = 1
),
contract_reads AS (
    SELECT
        contract_address,
        block_number,
        function_sig,
        function_name,
        function_input,
        DATA,
        utils.udf_json_rpc_call(
            'eth_call',
            [{ 'to': contract_address, 'from': null, 'data': data }, utils.udf_int_to_hex(block_number) ]
        ) AS rpc_request,
        live.udf_api(
            'POST',
            CONCAT(
                '{service}',
                '/',
                '{Authentication}'
            ),{},
            rpc_request,
            'vault/prod/blast/mainnet'
        ) AS read_output,
        SYSDATE() AS _inserted_timestamp
    FROM
        inputs
),
reads_flat AS (
    SELECT
        read_output,
        read_output :data :id :: STRING AS read_id,
        read_output :data :result :: STRING AS read_result,
        SPLIT(
            read_id,
            '-'
        ) AS read_id_object,
        function_sig,
        function_name,
        function_input,
        DATA,
        contract_address,
        block_number,
        _inserted_timestamp
    FROM
        contract_reads
)
SELECT
    read_output,
    read_id,
    read_result,
    read_id_object,
    function_sig,
    function_name,
    function_input,
    DATA,
    block_number,
    contract_address,
    CONCAT('0x', SUBSTR(read_result, 27, 40)) AS token_address,
    _inserted_timestamp
FROM
    reads_flat
WHERE
    token_address IS NOT NULL
