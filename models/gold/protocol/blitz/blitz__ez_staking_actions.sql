 {{ config(
    materialized = 'view',
    enabled = false,
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'VERTEX',
                'PURPOSE': 'CLOB, DEX, STAKING'
            }
        }
    }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    to_address,
    from_address,
    stake_action,
    symbol,
    amount_unadj,
    amount,
    amount_usd,
    blitz_staking_id,
    inserted_timestamp,
    modified_timestamp,
    blitz_staking_id
FROM
    {{ ref('silver__blitz_staking') }}