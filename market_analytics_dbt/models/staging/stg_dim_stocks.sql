{{
    config(
        materialized='view',
        schema='staging'
    )
}}

with source as (
    select * from {{ source('raw', 'DIM_STOCKS') }}
),

cleaned as (
    select
        SYMBOL,
        NAME,
        SECTOR,
        EXCHANGE,
        CURRENCY,
        IS_CURRENT,
        VALID_FROM,
        VALID_TO
        
    from source
    where IS_CURRENT = true
)

select * from cleaned