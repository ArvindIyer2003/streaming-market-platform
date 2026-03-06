{{
    config(
        materialized='view',
        schema='staging'
    )
}}

with source as (
    select * from {{ source('raw', 'STOCK_TRADES') }}
),

cleaned as (
    select
        SYMBOL,
        TIMESTAMP_PARSED,
        TRADE_DATE,
        SECTOR,
        EXCHANGE,
        CURRENCY,
        
        -- Price metrics
        PRICE,
        PRICE_CHANGE,
        PRICE_CHANGE_PCT,
        VOLUME,
        
        -- Flags
        IS_ANOMALY,
        IS_PRICE_SPIKE,
        IS_HIGH_VOLUME,
        PRICE_DIRECTION,
        VOLATILITY
        
    from source
    where PRICE is not null
      and PRICE > 0
      and VOLUME >= 0
)

select * from cleaned