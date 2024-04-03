 {{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'VERTEX',
                'PURPOSE': 'CLOB, DEX'
            }
        }
    }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    edge_event_index,
    user_event_index,
    edge_digest,
    user_digest,
    trader,
    subaccount,
    symbol,
    edge_order_type,
    user_order_type,
    edge_trade_type,
    user_trade_type,
    edge_is_taker,
    user_is_taker,
    edge_price_amount_unadj,
    user_price_amount_unadj,
    edge_price_amount,
    user_price_amount,
    edge_amount_unadj,
    user_amount_unadj,
    edge_amount,
    user_amount,
    edge_amount_usd,
    user_amount_usd,
    edge_fee_amount_unadj,
    user_fee_amount_unadj,
    edge_fee_amount,
    user_fee_amount,
    edge_base_delta_unadj,
    user_base_delta_unadj,
    edge_base_delta,
    user_base_delta,
    edge_quote_delta_unadj,
    user_quote_delta_unadj,
    edge_quote_delta,
    user_quote_delta,
    blitz_edge_trade_id,
    inserted_timestamp,
    modified_timestamp,
FROM
    {{ ref('silver__blitz_edge_trades') }}