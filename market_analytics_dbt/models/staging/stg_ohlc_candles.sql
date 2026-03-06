{{
    config(
        materialized='view',
        schema='staging'
    )
}}

with source as (
    select * from {{ source('raw', 'OHLC_CANDLES') }}
),

cleaned as (
    select
        SYMBOL,
        WINDOW_START,
        WINDOW_END,
        SECTOR,
        EXCHANGE,
        CURRENCY,
        
        -- OHLC
        OPEN,
        HIGH,
        LOW,
        CLOSE,
        VOLUME,
        AVG_PRICE,
        TICK_COUNT,
        
        -- Derived metrics
        MAX_CHANGE_PCT,
        MIN_CHANGE_PCT,
        ANOMALY_COUNT,
        CANDLE_COLOR,
        CANDLE_RANGE_PCT,
        
        -- Calculate additional metrics
        (HIGH - LOW) as PRICE_RANGE,
        (CLOSE - OPEN) as PRICE_MOVEMENT,
        case
            when OPEN != 0 then ((CLOSE - OPEN) / OPEN) * 100
            else 0
        end as CANDLE_CHANGE_PCT
        
    from source
    where OPEN is not null
      and HIGH >= LOW
      and CLOSE is not null
)

select * from cleaned