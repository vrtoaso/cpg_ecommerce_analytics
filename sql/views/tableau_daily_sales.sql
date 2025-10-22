WITH daily_aggregated AS (
  -- First, aggregate the data as you currently do
  SELECT 
    DATE(order_date) as order_date,
    EXTRACT(YEAR FROM DATE(order_date)) as year,
    EXTRACT(QUARTER FROM DATE(order_date)) as quarter,
    EXTRACT(MONTH FROM DATE(order_date)) as month_num,
    FORMAT_DATE('%B', DATE(order_date)) as month_name,
    FORMAT_DATE('%Y-%m', DATE(order_date)) as year_month,
    
    product_sku,
    product_name,
    marketing_channel,
    COALESCE(partner_name, 'Direct') as partner_name,
    
    -- Aggregated metrics
    SUM(adjusted_quantity) as units_sold,
    COUNT(DISTINCT order_id) as order_count,
    COUNT(DISTINCT customer_id) as customer_count,
    ROUND(SUM(unit_price * adjusted_quantity), 2) as gross_revenue,
    ROUND(SUM(line_discount), 2) as discount_amount,
    ROUND(SUM(line_revenue - line_discount), 2) as net_revenue,
    ROUND(SUM(cost * adjusted_quantity), 2) as product_cost,
    ROUND(SUM(allocated_shipping), 2) as shipping_cost,
    ROUND(SUM(allocated_duties), 2) as duties_cost,
    ROUND(SUM(line_revenue - line_discount - (cost * adjusted_quantity)), 2) as gross_profit,
    ROUND(SUM(line_revenue - line_discount - (cost * adjusted_quantity) - allocated_shipping - allocated_duties), 2) as contribution_profit,
    
    -- Percentage metrics
    ROUND(SAFE_DIVIDE(
      SUM(line_revenue - line_discount - (cost * adjusted_quantity)),
      SUM(line_revenue - line_discount)
    ) * 100, 2) as gross_margin_pct,
    
    ROUND(SAFE_DIVIDE(
      SUM(line_revenue - line_discount - (cost * adjusted_quantity) - allocated_shipping - allocated_duties),
      SUM(line_revenue - line_discount)
    ) * 100, 2) as contribution_margin_pct,
    
    ROUND(AVG(unit_price), 2) as avg_unit_price,
    ROUND(AVG(cost), 2) as avg_unit_cost
    
  FROM IBP.order_details
  WHERE transaction_type = 'order'
    AND DATE(order_date) >= '2023-01-01'
    AND DATE(order_date) <= '2023-12-31'
  GROUP BY 
    DATE(order_date),
    EXTRACT(YEAR FROM DATE(order_date)),
    EXTRACT(QUARTER FROM DATE(order_date)),
    EXTRACT(MONTH FROM DATE(order_date)),
    FORMAT_DATE('%B', DATE(order_date)),
    FORMAT_DATE('%Y-%m', DATE(order_date)),
    product_sku,
    product_name,
    marketing_channel,
    COALESCE(partner_name, 'Direct')
)
-- Now add the LAG calculations
SELECT 
  *,
  -- Add 30-day comparison metrics
  LAG(net_revenue, 30) OVER (
    PARTITION BY product_sku, marketing_channel, partner_name 
    ORDER BY order_date
  ) as net_revenue_30d_ago,
  
  LAG(units_sold, 30) OVER (
    PARTITION BY product_sku, marketing_channel, partner_name 
    ORDER BY order_date
  ) as units_sold_30d_ago,
  
  -- Calculate the percentage change
  ROUND(SAFE_DIVIDE(
    net_revenue - LAG(net_revenue, 30) OVER (PARTITION BY product_sku, marketing_channel, partner_name ORDER BY order_date),
    LAG(net_revenue, 30) OVER (PARTITION BY product_sku, marketing_channel, partner_name ORDER BY order_date)
  ) * 100, 2) as revenue_change_30d_pct
  
FROM daily_aggregated