{{
    config(
        materialized='table',
        schema='marts'
    )
}}

with candles as (
    select * from {{ ref('stg_ohlc_candles') }}
),

sentiment as (
    select * from {{ ref('stg_sentiment') }}
),

-- Get latest prices
latest_prices as (
    select
        SYMBOL,
        CLOSE as CURRENT_PRICE,
        row_number() over (partition by SYMBOL order by WINDOW_START desc) as rn
    from candles
),

-- Aggregate last 24 hours
stock_metrics as (
    select
        SYMBOL,
        max(SECTOR) as SECTOR,
        max(EXCHANGE) as EXCHANGE,
        
        -- Last 24 hours
        count(*) as CANDLE_COUNT_24H,
        avg(CLOSE) as AVG_PRICE_24H,
        stddev(CLOSE) as PRICE_VOLATILITY_24H,
        sum(VOLUME) as TOTAL_VOLUME_24H,
        
        -- Price movement
        max(HIGH) as HIGH_24H,
        min(LOW) as LOW_24H,
        
        current_timestamp() as CALCULATED_AT
        
    from candles
    where WINDOW_START >= dateadd(hour, -24, current_timestamp())
    group by SYMBOL
),

-- Get sentiment
sentiment_agg as (
    select
        SYMBOL,
        avg(NEWS_SENTIMENT) as AVG_SENTIMENT_24H,
        sum(NEWS_COUNT) as TOTAL_NEWS_24H,
        avg(POSITIVE_RATIO) as AVG_POSITIVE_RATIO
        
    from sentiment
    where TIMESTAMP_PARSED >= dateadd(hour, -24, current_timestamp())
    group by SYMBOL
),

-- Join with latest price
with_current_price as (
    select
        sm.*,
        lp.CURRENT_PRICE
    from stock_metrics sm
    left join latest_prices lp
        on sm.SYMBOL = lp.SYMBOL
        and lp.rn = 1
),

-- Join with sentiment
final as (
    select
        wcp.*,
        coalesce(sa.AVG_SENTIMENT_24H, 0) as AVG_SENTIMENT_24H,
        coalesce(sa.TOTAL_NEWS_24H, 0) as TOTAL_NEWS_24H,
        coalesce(sa.AVG_POSITIVE_RATIO, 0) as AVG_POSITIVE_RATIO
        
    from with_current_price wcp
    left join sentiment_agg sa
        on wcp.SYMBOL = sa.SYMBOL
)

select * from final