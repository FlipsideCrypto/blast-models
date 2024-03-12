 {{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'BLITZ',
                'PURPOSE': 'CLOB, DEX, CLEARINGHOUSE'
            }
        }
    }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    event_name,
    event_index,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    modification_type,
    symbol,
    trader,
    subaccount,
    token_address,
    amount_unadj,
    amount,
    amount_usd,
    blitz_collateral_id AS ez_clearing_house_events_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__blitz_collateral') }}