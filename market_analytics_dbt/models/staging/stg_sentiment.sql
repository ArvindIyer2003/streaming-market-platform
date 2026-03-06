{{
    config(
        materialized='view',
        schema='staging'
    )
}}

with source as (
    select * from {{ source('raw', 'STOCK_SENTIMENT') }}
),

cleaned as (
    select
        SYMBOL,
        TIMESTAMP_PARSED,
        PRICE,
        PRICE_CHANGE_PCT,
        
        -- Sentiment metrics
        NEWS_SENTIMENT,
        NEWS_COUNT,
        POSITIVE_NEWS_COUNT,
        NEGATIVE_NEWS_COUNT,
        
        -- Categorize sentiment
        case
            when NEWS_SENTIMENT > 0.3 then 'POSITIVE'
            when NEWS_SENTIMENT < -0.3 then 'NEGATIVE'
            else 'NEUTRAL'
        end as SENTIMENT_CATEGORY,
        
        -- Calculate sentiment ratio
        case
            when NEWS_COUNT > 0 then 
                (POSITIVE_NEWS_COUNT::float / NEWS_COUNT) * 100
            else 0
        end as POSITIVE_RATIO
        
    from source
    where NEWS_SENTIMENT is not null
)

select * from cleaned