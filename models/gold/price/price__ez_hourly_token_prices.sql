{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    HOUR,
    token_address,
    symbol,
    decimals,
    price,
    is_imputed,
    hourly_prices_priority_id AS ez_hourly_token_prices_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__hourly_prices_priority') }}
