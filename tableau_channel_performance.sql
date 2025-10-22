SELECT 
  FORMAT_DATE('%Y-%m', DATE(order_date)) as year_month,
  marketing_channel,
  
  -- Core metrics
  COUNT(DISTINCT customer_id) as unique_customers,
  COUNT(DISTINCT order_id) as total_orders,
  SUM(adjusted_quantity) as units_sold,
  
  -- Financial metrics
  ROUND(SUM(line_revenue - line_discount), 2) as net_revenue,
  ROUND(SUM((line_revenue - line_discount) - (cost * adjusted_quantity)), 2) as gross_profit,
  ROUND(SUM((line_revenue - line_discount) - (cost * adjusted_quantity) - allocated_shipping - allocated_duties), 2) as contribution_profit,
  
  -- Efficiency metrics (pre-calculated for Tableau)
  ROUND(SAFE_DIVIDE(
    SUM(line_revenue - line_discount),
    COUNT(DISTINCT order_id)
  ), 2) as avg_order_value,
  
  ROUND(SAFE_DIVIDE(
    SUM(adjusted_quantity),
    COUNT(DISTINCT order_id)
  ), 2) as units_per_order,
  
  -- Margin percentage
  ROUND(SAFE_DIVIDE(
    SUM((line_revenue - line_discount) - (cost * adjusted_quantity)),
    SUM(line_revenue - line_discount)
  ) * 100, 2) as gross_margin_pct
  
FROM IBP.order_details
WHERE transaction_type = 'order'
  AND DATE(order_date) >= '2023-01-01'
  AND DATE(order_date) <= '2023-12-31'
GROUP BY 
  FORMAT_DATE('%Y-%m', DATE(order_date)),
  marketing_channel