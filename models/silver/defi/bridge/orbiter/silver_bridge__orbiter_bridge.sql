{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

with bridge_router as (
-- for  bridge tx utilizing router
select
    block_number,
    block_timestamp,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_hash,
    contract_address,
    event_index,
    origin_to_address as to_address,
    origin_from_address as from_address,
    origin_from_address as depositor, 
    TRY_TO_NUMBER(right(utils.udf_hex_to_int(data :: STRING), 4)) as destinationChainId,
    TRY_TO_NUMBER(utils.udf_hex_to_int(data :: STRING)*pow(10,-18)) as value,
    origin_from_address as receipient,
    concat('0x', substr(topics[1]::STRING, 27, 40)) as bridge_address,
    CASE
        WHEN tx_status = 'success' THEN TRUE
        ELSE FALSE
    END AS tx_succeeded,
    CONCAT(
        tx_hash,
        '-',
        event_index
    ) AS _log_id,
    modified_timestamp
from {{ ref('core__fact_event_logs') }}
where 
1=1
--and bridge_address in ('0xe4edb277e41dc89ab076a1f049f4a3efa700bce8', '0xee73323912a4e3772b74ed0ca1595a152b0ef282') -- orbiter bridge
and topics[0]::STRING ='0x69ca02dd4edd7bf0a4abb9ed3b7af3f14778db5d61921c7dc7cd545266326de2'
and contract_address='0x13e46b2a3f8512ed4682a8fb8b560589fe3c2172'
        AND tx_succeeded

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}

),

{# bridge_native as (
-- for direct native eth transfers
select 
    block_number,
    block_timestamp,
    tx_hash,
    to_address,
    from_address,
    from_address as depositor, 
    right(value_precise_raw, 4) as destinationChainId,
    value,
    from_address as receipient,
    to_address as bridge_address,
    CASE
        WHEN status = 'SUCCESS' THEN TRUE
        ELSE FALSE
    END AS tx_succeeded,
    modified_timestamp
from {{ ref('core__fact_transactions') }} 
where to_address in ('0xe4edb277e41dc89ab076a1f049f4a3efa700bce8','0x80c67432656d59144ceff962e8faf8926599bcf8')
and tx_succeeded

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),

bridge_combine as (
select * from bridge_native 
union all 
select * from bridge_router
)

select 
    *, 
    'orbiter finance' as platform,
    '0x4300000000000000000000000000000000000004' as token_address, -- hardcoded weth contract address
    value as token_amount -- native eth
from 
    bridge_combine #}