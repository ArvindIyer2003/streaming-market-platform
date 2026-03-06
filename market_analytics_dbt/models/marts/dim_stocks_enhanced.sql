{{
    config(
        materialized='table',
        schema='marts'
    )
}}

with stocks as (
    select * from {{ ref('stg_dim_stocks') }}
),

-- Get latest trade info per symbol
latest_trades_ranked as (
    select
        SYMBOL,
        PRICE as LATEST_PRICE,
        TIMESTAMP_PARSED as LAST_TRADE_TIME,
        row_number() over (partition by SYMBOL order by TIMESTAMP_PARSED desc) as rn
    from {{ ref('stg_stock_trades') }}
),

latest_trades as (
    select
        SYMBOL,
        LATEST_PRICE,
        LAST_TRADE_TIME
    from latest_trades_ranked
    where rn = 1
),

enhanced as (
    select
        s.SYMBOL,
        s.NAME,
        s.SECTOR,
        s.EXCHANGE,
        s.CURRENCY,
        
        -- Latest data
        t.LATEST_PRICE,
        t.LAST_TRADE_TIME,
        
        -- Flags
        case
            when t.LAST_TRADE_TIME >= dateadd(hour, -1, current_timestamp())
            then true else false
        end as IS_ACTIVELY_TRADING,
        
        current_timestamp() as UPDATED_AT
        
    from stocks s
    left join latest_trades t
        on s.SYMBOL = t.SYMBOL
)

select * from enhanced