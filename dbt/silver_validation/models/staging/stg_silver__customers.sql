select
  customer_id,
  email,
  country_code,
  created_at,
  signup_date
from {{ source('silver', 'customers') }}

