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
    product_id,
    orderbook_side,
    volume,
    price,
    round_price_0_01,
    round_price_0_1,
    round_price_1,
    round_price_10,
    round_price_100,
    blitz_market_depth_id as ez_market_depth_stats_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__blitz_market_depth') }}