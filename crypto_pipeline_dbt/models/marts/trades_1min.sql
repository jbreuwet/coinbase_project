with silver as (
    select
        trade_id,
        product_id,
        side,
        price,
        size,
        traded_at
    from {{ ref('stg_trades') }}
),

windowed as (
    select
        date_trunc('minute', traded_at) as window_start,
        date_trunc('minute', traded_at) + interval '1 min' as window_end,
        product_id,
        count(*) as trade_count,
        sum(price * size) / nullif(sum(size), 0) as vwap,
        min(price) as low_price,
        max(price) as high_price,
        sum(size) as total_volume,
        sum(case when side = 'BUY' then size else 0 end) as buy_volume,
        sum(case when side = 'SELL' then size else 0 end) as sell_volume
    from silver
    group by
        date_trunc('minute', traded_at),
        product_id
)

select
    window_start,
    window_end,
    product_id,
    trade_count,
    vwap,
    low_price,
    high_price,
    total_volume,
    buy_volume,
    sell_volume
from windowed
order by window_start desc