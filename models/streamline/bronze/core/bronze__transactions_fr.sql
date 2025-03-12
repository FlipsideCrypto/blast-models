{{ config (
    materialized = 'view',
    tags = ['bronze_core']
) }}

SELECT
    partition_key,
    block_number,
    VALUE,
    DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
    {{ ref('bronze__transactions_fr_v2') }}
UNION ALL
SELECT
    partition_key,
    block_number,
    VALUE,
    DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
    {{ ref('bronze__transactions_fr_v1') }}
