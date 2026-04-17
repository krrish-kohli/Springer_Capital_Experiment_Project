INSERT INTO raw.customers (customer_id, email, country_code, created_at, signup_date) VALUES
  (1, 'alice@example.com', 'US', now() - INTERVAL 10 DAY, toDate(now()) - 10),
  (2, 'bob@example.com',   'UNK', now() - INTERVAL 9 DAY,  toDate('1970-01-01')),
  (3, '',                 'CA', now() - INTERVAL 8 DAY,  toDate(now()) - 8),
  (4, 'dina@example.com',  'US', now() - INTERVAL 7 DAY,  toDate(now()) - 7);

-- Silver has issues:
-- - customer_id=2 has suspicious defaults (country_code=UNK, signup_date=1970-01-01)
-- - customer_id=3 missing required email (empty)
-- - customer_id=4 duplicated
-- - customer_id=1 missing (missing key)
INSERT INTO silver.customers (customer_id, email, country_code, created_at, signup_date) VALUES
  (2, 'bob@example.com',  'UNK', now() - INTERVAL 9 DAY,  toDate('1970-01-01')),
  (3, '',                'CA',  now() - INTERVAL 8 DAY,  toDate(now()) - 8),
  (4, 'dina@example.com', 'US',  now() - INTERVAL 7 DAY,  toDate(now()) - 7),
  (4, 'dina@example.com', 'US',  now() - INTERVAL 7 DAY,  toDate(now()) - 7);

INSERT INTO raw.orders (order_id, customer_id, order_total, order_ts, currency) VALUES
  (10, 1,  25.50, now() - INTERVAL 5 DAY, 'USD'),
  (11, 2,  99.99, now() - INTERVAL 4 DAY, 'USD'),
  (12, 999, 10.00, now() - INTERVAL 3 DAY, 'USD'); -- invalid customer_id

-- Silver orders has issues:
-- - order_id=11 duplicated
-- - order_id=12 missing (missing key from raw)
-- - order_id=13 has missing FK to customers
INSERT INTO silver.orders (order_id, customer_id, order_total, order_ts, currency) VALUES
  (10, 1,   25.50, now() - INTERVAL 5 DAY, 'USD'),
  (11, 2,   99.99, now() - INTERVAL 4 DAY, 'USD'),
  (11, 2,   99.99, now() - INTERVAL 4 DAY, 'USD'),
  (13, 999, 10.00, now() - INTERVAL 3 DAY, 'USD');

