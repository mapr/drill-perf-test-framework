SELECT ROW_NUMBER() OVER (PARTITION BY ss.ss_store_sk ORDER BY ss.ss_customer_sk) FROM store_sales ss LIMIT 10;

