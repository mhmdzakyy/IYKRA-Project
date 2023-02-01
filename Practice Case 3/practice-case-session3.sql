-- Muhammad Zaky Aonillah
-- Pracice Case Session 3

WITH 
  -- remove duplicate data
  cleaned_table AS (
    SELECT DISTINCT * 
    FROM `data-to-insights.ecommerce.rev_transactions` 
  ),

  -- get total transaction per date and country based on channel groupping
  repeated_column AS (
    SELECT
      channelGrouping, 
      geoNetwork_country, 
      PARSE_DATE("%Y%m%d", date) AS date, 
      SUM(totals_transactions) as totalTransactions
    FROM `data-to-insights.ecommerce.rev_transactions` 
    GROUP BY 1,2,3
    ORDER BY 1,2,3 ASC
  ),

  -- create nested column 
  nested_column AS (
    SELECT 
      channelGrouping,
      geoNetwork_country,
      ARRAY_AGG(date ORDER BY date) as transaction_date,
      ARRAY_AGG(totalTransactions ORDER BY totalTransactions) as totalTransactions
    FROM repeated_column
    GROUP BY 1, 2
    ORDER BY 1
)


SELECT * FROM nested_column

