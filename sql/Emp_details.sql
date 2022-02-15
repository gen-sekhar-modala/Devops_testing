CREATE OR REPLACE TABLE `data-engineering-gcp-practice.Example_dataset1.Emp_details`
(
  timestamp TIMESTAMP,
  customer_id STRING,
  transaction_amount NUMERIC,
  customer_name STRING,
  location STRING
)
PARTITION BY DATE(timestamp)
CLUSTER BY customer_id;