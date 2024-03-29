{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    tx_hash,
    block_number,
    block_timestamp,
    from_address,
    to_address,
    eth_value AS VALUE,
    eth_value_precise_raw AS value_precise_raw,
    eth_value_precise AS value_precise,
    gas,
    gas_used,
    input,
    output,
    TYPE,
    identifier,
    DATA,
    tx_status,
    sub_traces,
    trace_status,
    error_reason,
    trace_index,
    traces_id AS fact_traces_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__traces') }}
