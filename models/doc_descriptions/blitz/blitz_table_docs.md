{% docs blitz_dim_products %}

All available Blitz products, these are automatically added as they are released on chain.


{% enddocs %}

{% docs blitz_ez_liquidations %}

All Blitz liquidations. Once an account’s maintenance margin reaches $0, the account is eligible for liquidation. Liquidation events happen one by one, with the riskiest positions being liquidated first. Liquidations are based on the oracle price.


{% enddocs %}

{% docs blitz_ez_perp_trades %}

Blitz perpetuals are derivative contracts on an underlying spot asset. On Blitz, all perpetual contracts trade against USDC.

{% enddocs %}

{% docs blitz_ez_spot_trades %}

Blitz’s spot markets allow you to buy or sell listed crypto assets paired with USD-denominated stablecoins.

{% enddocs %}

{% docs blitz_ez_clearing_house_events %}

Blitz’s on-chain clearinghouse operates as the hub combining perpetual and spot markets, collateral, and risk calculations into a single integrated system. The events in this table track when a wallet either deposits or withdraws from the clearinghouse contract.

{% enddocs %}

{% docs blitz_ez_account_stats %}

Subaccount level table showing aggregated total activity across the Blitz exchange.

{% enddocs %}

{% docs blitz_ez_market_stats %}

Orderbook level market stats based on a combination of on-chain data and data from Blitz's ticker V2 API which includes 24-hour pricing and volume information on each market pair available on Blitz.

{% enddocs %}

{% docs blitz_ez_market_depth %}

Liquidity data taken from Blitz's Orderbook API, showing amount of liquidity at each price level.

{% enddocs %}

{% docs blitz_ez_staking  %}

All staking actions taken with the VRTX staking contract.

{% enddocs %}

{% docs blitz_ez_edge_trades  %}

All edge trades paired with the associated trader/subaccount.

{% enddocs %}