WITH current_month AS (
  SELECT MAX(DATE_TRUNC(DATE(order_date), MONTH)) as max_month
  FROM IBP.order_details
  WHERE transaction_type = 'order'
    AND DATE(order_date) >= '2023-01-01'
    AND DATE(order_date) <= '2023-12-31'
),
monthly_metrics AS (
  SELECT 
    DATE_TRUNC(DATE(order_date), MONTH) as month,
    
    -- Revenue metrics
    SUM(line_revenue - line_discount) as net_revenue,
    SUM((line_revenue - line_discount) - (cost * adjusted_quantity)) as gross_profit,
    SUM(adjusted_quantity) as units_sold,
    COUNT(DISTINCT customer_id) as unique_customers,
    
    -- Calculate margins
    SAFE_DIVIDE(
      SUM((line_revenue - line_discount) - (cost * adjusted_quantity)),
      SUM(line_revenue - line_discount)
    ) * 100 as gross_margin_pct
    
  FROM IBP.order_details
  WHERE transaction_type = 'order'
    AND DATE(order_date) >= '2023-01-01'
    AND DATE(order_date) <= '2023-12-31'
  GROUP BY DATE_TRUNC(DATE(order_date), MONTH)
),
metrics_with_lag AS (
  SELECT 
    m.month,
    m.net_revenue,
    m.gross_profit,
    m.gross_margin_pct,
    m.units_sold,
    m.unique_customers,
    
    -- YoY calculations
    LAG(m.net_revenue, 12) OVER (ORDER BY m.month) as net_revenue_yoy,
    LAG(m.units_sold, 12) OVER (ORDER BY m.month) as units_sold_yoy,
    
    -- MoM calculations  
    LAG(m.net_revenue, 1) OVER (ORDER BY m.month) as net_revenue_mom,
    LAG(m.units_sold, 1) OVER (ORDER BY m.month) as units_sold_mom
    
  FROM monthly_metrics m
)
SELECT 
  mwl.month,
  CASE 
    WHEN mwl.month = cm.max_month THEN 'Current Month'
    WHEN mwl.month = DATE_SUB(cm.max_month, INTERVAL 1 MONTH) THEN 'Previous Month'
    WHEN mwl.month = DATE_SUB(cm.max_month, INTERVAL 12 MONTH) THEN 'Year Ago'
    ELSE 'Historical'
  END as period_label,
  
  -- Metrics
  ROUND(mwl.net_revenue, 0) as net_revenue,
  ROUND(mwl.gross_profit, 0) as gross_profit,
  ROUND(mwl.gross_margin_pct, 1) as gross_margin_pct,
  mwl.units_sold,
  mwl.unique_customers,
  
  -- Period comparisons
  mwl.net_revenue_yoy,
  mwl.units_sold_yoy,
  mwl.net_revenue_mom,
  mwl.units_sold_mom
  
FROM metrics_with_lag mwl
CROSS JOIN current_month cm
ORDER BY mwl.month DESC