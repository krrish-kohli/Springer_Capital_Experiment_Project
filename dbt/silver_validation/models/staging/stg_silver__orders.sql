select
  order_id,
  customer_id,
  order_total,
  order_ts,
  currency
from {{ source('silver', 'orders') }}

