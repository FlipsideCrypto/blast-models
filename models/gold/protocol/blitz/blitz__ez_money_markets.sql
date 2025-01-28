 {{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'BLITZ',
                'PURPOSE': 'CLOB, DEX, STATS'
            }
        }
    }
) }}

SELECT
    hour,
    ticker_id,
    symbol,
    product_id,
    deposit_apr,
    borrow_apr,
    tvl,
    blitz_money_markets_id as ez_money_markets_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__blitz_money_markets') }}