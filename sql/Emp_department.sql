CREATE OR REPLACE TABLE `data-engineering-gcp-practice.Example_dataset1.Emp_department`
(
  ID INT64 NOT NULL OPTIONS(description="Emp id"),
  Department STRING OPTIONS(description="dep name")
);