 {{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'BLITZ',
                'PURPOSE': 'CLOB, DEX, PRODUCTS'
            }
        }
    }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    product_id,
    product_type,
    ticker_id,
    symbol,
    name,
    blitz_products_id AS dim_products_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__blitz_dim_products') }}
ORDER BY product_id