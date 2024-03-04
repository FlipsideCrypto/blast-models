{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    token_address,
    id,
    symbol,
    NAME,
    decimals,
    provider,
    asset_metadata_all_providers_id AS dim_asset_metadata_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__asset_metadata_all_providers') }}
