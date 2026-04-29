with source as (
    select
        id,
        raw_payload,
        ingested_at
    from {{ source('bronze', 'trades') }}
),

cleaned as (
    select
        id,
        (raw_payload->>'trade_id')::bigint as trade_id,
        (raw_payload->>'product_id')::text as product_id,
        (raw_payload->>'side')::text as side,
        (raw_payload->>'price')::numeric(18,8) as price,
        (raw_payload->>'size')::numeric(18,8) as size,
        (raw_payload->>'time')::timestamptz as traded_at,
        ingested_at
    from source
),

deduplicated as (
    select distinct on (trade_id)
        id,
        trade_id,
        product_id,
        side,
        price,
        size,
        traded_at,
        ingested_at
    from cleaned
    order by trade_id, traded_at
)

select
    id,
    trade_id,
    product_id,
    side,
    price,
    size,
    traded_at,
    ingested_at
from deduplicated