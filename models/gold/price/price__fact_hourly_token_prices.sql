{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    HOUR,
    token_address,
    price,
    is_imputed,
    provider,
    hourly_prices_all_providers_id AS fact_hourly_token_prices_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__hourly_prices_all_providers') }}
