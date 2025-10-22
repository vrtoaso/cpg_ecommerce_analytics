WITH product_metrics AS (
  SELECT 
    product_sku,
    product_name,
    FORMAT_DATE('%Y-%m', DATE(order_date)) as year_month,
    
    -- Volume and revenue
    SUM(adjusted_quantity) as units_sold,
    SUM(line_revenue - line_discount) as net_revenue,
    SUM((line_revenue - line_discount) - (cost * adjusted_quantity)) as gross_profit
    
  FROM IBP.order_details
  WHERE transaction_type = 'order'
    AND DATE(order_date) >= '2023-01-01'
    AND DATE(order_date) <= '2023-12-31'
  GROUP BY product_sku, product_name, FORMAT_DATE('%Y-%m', DATE(order_date))
),
ranked_products AS (
  SELECT 
    *,
    -- Ranking within each month
    RANK() OVER (
      PARTITION BY year_month 
      ORDER BY net_revenue DESC
    ) as revenue_rank
  FROM product_metrics
)
SELECT 
  product_sku,
  product_name,
  year_month,
  units_sold,
  net_revenue,
  gross_profit,
  
  -- Pre-calculated percentages
  ROUND(SAFE_DIVIDE(gross_profit, net_revenue) * 100, 2) as gross_margin_pct,
  
  -- Ranking and categorization
  revenue_rank,
  CASE 
    WHEN revenue_rank <= 5 THEN 'Top 5'
    WHEN revenue_rank <= 10 THEN 'Top 10'
    WHEN revenue_rank <= 20 THEN 'Top 20'
    ELSE 'Other'
  END as product_tier,
  
  -- Period over period (for Tableau to use directly)
  LAG(units_sold, 1) OVER (PARTITION BY product_sku ORDER BY year_month) as prev_month_units,
  LAG(net_revenue, 1) OVER (PARTITION BY product_sku ORDER BY year_month) as prev_month_revenue
  
FROM ranked_products