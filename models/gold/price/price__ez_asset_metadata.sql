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
    asset_metadata_priority_id AS ez_asset_metadata_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__asset_metadata_priority') }}
