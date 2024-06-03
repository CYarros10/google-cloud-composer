-- 1. Creating the table 
CREATE OR REPLACE TABLE  cy-artifacts.sandbox.test (
  column1 STRING,
  column2 INT64,
  column3 FLOAT64
);

-- 2. Inserting Data
INSERT INTO cy-artifacts.sandbox.test (column1, column2, column3)
VALUES
  ('Sample Value 1', 10, 3.14),
  ('Sample Value 2', 25, 2.71),
  ('Sample Value 3', 5, 1.618); 