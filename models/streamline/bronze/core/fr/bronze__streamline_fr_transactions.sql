{{ config (
    materialized = 'view'
) }}

{{ streamline_external_table_fr_query_v2(
    model = 'transactions',
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER )"
) }}
