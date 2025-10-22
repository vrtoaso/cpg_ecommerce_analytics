WITH order_level_data AS (
  SELECT 
    DATE(order_date) as order_date,
    FORMAT_DATE('%Y-%m', DATE(order_date)) as year_month,
    product_sku,
    product_name,
    marketing_channel,
    order_id,
    
    -- Aggregations at order level first
    SUM(adjusted_quantity) as order_units,
    SUM(line_revenue) as order_gross_revenue,
    SUM(line_discount) as order_discount,
    SUM(line_revenue - line_discount) as order_net_revenue,
    AVG(unit_price) as order_avg_price
    
  FROM IBP.order_details
  WHERE transaction_type = 'order'
    AND DATE(order_date) >= '2023-01-01'
    AND DATE(order_date) <= '2023-12-31'
  GROUP BY 
    DATE(order_date),
    FORMAT_DATE('%Y-%m', DATE(order_date)),
    product_sku,
    product_name,
    marketing_channel,
    order_id
)
SELECT 
  order_date,
  year_month,
  product_sku,
  product_name,
  marketing_channel,
  
  -- Identify if product had any discounts
  CASE 
    WHEN SUM(order_discount) > 0 THEN 'Discounted'
    ELSE 'Full Price'
  END as price_type,
  
  -- Aggregated metrics
  COUNT(DISTINCT order_id) as order_count,
  SUM(order_units) as units_sold,
  
  -- Pricing metrics
  ROUND(AVG(order_avg_price), 2) as avg_list_price,
  ROUND(SAFE_DIVIDE(SUM(order_net_revenue), SUM(order_units)), 2) as avg_selling_price,
  ROUND(SAFE_DIVIDE(SUM(order_discount), SUM(order_gross_revenue)) * 100, 2) as discount_rate,
  
  -- Revenue impact
  ROUND(SUM(order_gross_revenue), 2) as gross_revenue,
  ROUND(SUM(order_discount), 2) as total_discount,
  ROUND(SUM(order_net_revenue), 2) as net_revenue
  
FROM order_level_data
GROUP BY 
  order_date,
  year_month,
  product_sku,
  product_name,
  marketing_channel