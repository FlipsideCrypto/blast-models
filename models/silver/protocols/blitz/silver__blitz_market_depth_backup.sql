{{ config(
    materialized = 'incremental',
    full_refresh = false,
    unique_key = ['product_id','hour','price'],
    cluster_by = ['hour::DATE'],
    tags = 'curated'
) }}

WITH market_depth AS (
    SELECT
        ticker_id,
        product_id,
        HOUR,
        orderbook_side,
        price,
        round_price_0_01,
        round_price_0_1,
        round_price_1,
        round_price_10,
        round_price_100,
        volume,
        inserted_timestamp,
        blitz_market_depth_id,
        _invocation_id
    FROM
        {{ ref('silver__blitz_market_depth') }}
{% if is_incremental() %}
WHERE hour > (
    SELECT
        MAX(
            hour
        )
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    *,
    SYSDATE() AS modified_timestamp
FROM
    market_depth