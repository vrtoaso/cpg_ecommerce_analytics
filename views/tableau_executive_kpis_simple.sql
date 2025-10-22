WITH base AS (
  SELECT 
    DATE_TRUNC(DATE(order_date), MONTH) AS month,
    line_revenue,
    line_discount,
    cost,
    adjusted_quantity,
    customer_id,
    order_id
  FROM IBP.order_details
  WHERE transaction_type = 'order'
    AND DATE(order_date) BETWEEN '2023-01-01' AND '2023-12-31'
)
SELECT
  month,
  ROUND(SUM(line_revenue - line_discount), 0) AS net_revenue,
  ROUND(SUM((line_revenue - line_discount) - (cost * adjusted_quantity)), 0) AS gross_profit,
  SUM(adjusted_quantity) AS units_sold,
  COUNT(DISTINCT customer_id) AS unique_customers,
  COUNT(DISTINCT order_id) AS order_count,
  CASE 
    WHEN month = DATE('2023-12-01') THEN 'Current'
    WHEN month = DATE('2023-11-01') THEN 'Previous'
    ELSE 'Historical'
  END AS period_type
FROM base
GROUP BY month
ORDER BY month DESC