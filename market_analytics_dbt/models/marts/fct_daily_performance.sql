{{
    config(
        materialized='table',
        schema='marts'
    )
}}

with trades as (
    select * from {{ ref('stg_stock_trades') }}
),

-- First, get first and last prices per day using window functions
price_endpoints as (
    select
        SYMBOL,
        TRADE_DATE,
        PRICE,
        VOLUME,
        IS_ANOMALY,
        IS_PRICE_SPIKE,
        PRICE_DIRECTION,
        row_number() over (partition by SYMBOL, TRADE_DATE order by TIMESTAMP_PARSED asc) as rn_first,
        row_number() over (partition by SYMBOL, TRADE_DATE order by TIMESTAMP_PARSED desc) as rn_last
    from trades
),

first_prices as (
    select
        SYMBOL,
        TRADE_DATE,
        PRICE as DAY_OPEN
    from price_endpoints
    where rn_first = 1
),

last_prices as (
    select
        SYMBOL,
        TRADE_DATE,
        PRICE as DAY_CLOSE
    from price_endpoints
    where rn_last = 1
),

-- Now do normal aggregations
daily_agg as (
    select
        SYMBOL,
        TRADE_DATE,
        
        -- Price metrics
        min(PRICE) as DAY_LOW,
        max(PRICE) as DAY_HIGH,
        avg(PRICE) as DAY_AVG_PRICE,
        
        -- Volume
        sum(VOLUME) as TOTAL_VOLUME,
        
        -- Activity
        count(*) as TICK_COUNT,
        
        -- Volatility
        stddev(PRICE) as PRICE_VOLATILITY,
        
        -- Anomalies
        sum(case when IS_ANOMALY then 1 else 0 end) as ANOMALY_COUNT,
        sum(case when IS_PRICE_SPIKE then 1 else 0 end) as SPIKE_COUNT,
        
        -- Direction
        sum(case when PRICE_DIRECTION = 'UP' then 1 else 0 end) as UP_TICKS,
        sum(case when PRICE_DIRECTION = 'DOWN' then 1 else 0 end) as DOWN_TICKS,
        
        current_timestamp() as CREATED_AT
        
    from trades
    group by SYMBOL, TRADE_DATE
),

-- Join everything together
final as (
    select 
        da.*,
        fp.DAY_OPEN,
        lp.DAY_CLOSE,
        
        (lp.DAY_CLOSE - fp.DAY_OPEN) as DAY_CHANGE,
        case
            when fp.DAY_OPEN != 0 then ((lp.DAY_CLOSE - fp.DAY_OPEN) / fp.DAY_OPEN) * 100
            else 0
        end as DAY_CHANGE_PCT,
        
        case
            when lp.DAY_CLOSE > fp.DAY_OPEN then 'GREEN'
            when lp.DAY_CLOSE < fp.DAY_OPEN then 'RED'
            else 'DOJI'
        end as DAY_CANDLE_COLOR
        
    from daily_agg da
    left join first_prices fp
        on da.SYMBOL = fp.SYMBOL
        and da.TRADE_DATE = fp.TRADE_DATE
    left join last_prices lp
        on da.SYMBOL = lp.SYMBOL
        and da.TRADE_DATE = lp.TRADE_DATE
)

select * from final