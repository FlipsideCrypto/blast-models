{{ config (
    materialized = 'view'
) }}

{# {% set model = this.identifier.split("_") [-1] %} #}
{{ streamline_external_table_FR_query(
    model = 'receipts_testnet',
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER )",
    partition_name = "_partition_by_block_id",
    unique_key = "partition_key"
) }}
