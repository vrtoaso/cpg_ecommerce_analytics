# 🧠 CPG eCommerce Analytics Project

This project demonstrates a complete end-to-end analytics implementation — from data architecture and SQL-based modeling through to Tableau visualization — for a Consumer Packaged Goods (CPG) ecommerce business.

It showcases both **data engineering and analytical storytelling** capabilities across the data lifecycle: *data platform design → transformation → business logic → visualization.*

---

## 📊 Project Overview

The goal of this project is to unify multi-channel ecommerce data (Shopify, Amazon, Wholesale) into a single analytics layer and surface key financial and operational insights through Tableau.

Key business questions addressed:
- What are our monthly and daily revenue trends?
- Which channels and products drive the greatest contribution margin?
- How is profitability changing month-over-month?
- Which marketing partners deliver the best ROI?

---

## 🏗️ Data Platform & Modeling Layer (SQL)

This layer defines the warehouse schema and transformations that standardize ecommerce data.

**Highlights:**
- Built using **Google BigQuery SQL**
- Implements **star schema design**
- Includes **incremental load logic** for scalable refresh
- Business logic embedded directly in SQL for consistency and performance

**Core components:**
| File | Description |
|------|--------------|
| `00_schema_ddl.sql` | Creates base tables for fact and dimension models |
| `01_fact_sales_incremental.sql` | Incremental load logic for sales fact table |
| `02_dim_product.sql` | Product dimension (SKU-level attributes) |
| `03_dim_channel.sql` | Sales channel mapping |
| `05_view_tableau_integration.sql` | Business logic view optimized for Tableau integration |

This approach ensures that Tableau connects to *clean, business-ready data*, minimizing complexity within the visualization layer.

---

## 🧮 Business Logic Layer

Key metrics and calculations are built directly into SQL views:

- **Net Revenue:** `SUM(line_revenue - line_discount)`
- **Contribution Margin:** `SAFE_DIVIDE(net_revenue - cost_of_goods - shipping - duties, net_revenue)`
- **MoM Change:** `LAG()` and `LEAD()` window functions for period-over-period comparison
- **Channel/Partner Aggregations:** pre-aggregated for Tableau performance

These transformations ensure consistent definitions across all analytics tools.

---

## 📈 Analytics & Visualization Layer (Tableau)

**File:** `tableau/cpg_sales_dashboard.twbx`  
**Preview:**  
![Dashboard Preview](tableau/dashboard_preview.png)

**Purpose:**  
Reveal how operational and financial data converge to drive business outcomes.

**Dashboard Features:**
- **Monthly Net Revenue Trend:** 12-month rolling analysis
- **Top 10 Products by Revenue:** SKU-level performance
- **Channel Performance Matrix:** Profitability vs. order value by partner
- **Daily Revenue Heat Map:** 30-day trend visualization

This dashboard demonstrates the translation of SQL-based business logic into executive-ready analytics.

---

## 🧩 Technical Integration: SQL + Tableau

This project exemplifies how structuring SQL for Tableau drives clarity and performance:

| Benefit | Implementation |
|----------|----------------|
| **Performance** | Pre-aggregated metrics reduce Tableau’s query load |
| **Consistency** | All metrics derived from SQL views |
| **Maintainability** | Logic centralized in one layer |
| **Ease of Use** | Tableau connects to flattened, analysis-ready tables |

---

## 🧑‍💻 Skills Demonstrated

| Area | Description |
|------|--------------|
| **Data Architecture** | Modeled fact/dimension schema in BigQuery |
| **ETL/ELT Engineering** | Created DDL and incremental insert scripts |
| **SQL Development** | Built analytical and business logic views |
| **Visualization** | Designed interactive Tableau dashboard |
| **Analytics Translation** | Bridged technical data design with business KPIs |

This project highlights expertise across **both the data platform layer** (engineering, modeling, transformation) and **the presentation layer** (analytics, visualization, insight delivery).

---

## 🧭 How to Explore

1. **View the Tableau Dashboard (Extract version)**  
   Download the `.twbx` file in `/tableau` and open in [Tableau Reader](https://www.tableau.com/products/reader) or Tableau Desktop.

2. **Review SQL Logic**  
   Explore the `/sql` folder to understand the data modeling and calculation logic behind each visualization.

3. **Understand the Architecture**  
   See `/docs/data_model_diagram.png` for a high-level view of the warehouse schema and Tableau integration.

---

## 🧠 Project Context

This project draws from real-world experience building data and analytics platforms for ecommerce and CPG organizations. It integrates lessons from roles spanning:
- **Data Architecture & Warehousing**
- **Analytics Engineering**
- **Operational and Financial Analytics**
- **Retail/CPG Planning and Partner Performance**

It illustrates a data professional’s ability to bridge *technical data engineering* and *strategic analytics storytelling* — enabling business teams to make data-driven decisions confidently.

---

**Author:** [Vincent Toaso](https://github.com/vrtoaso)  
**Tools Used:** BigQuery, SQL, Tableau, GitHub  
**Contact:** [LinkedIn](https://www.linkedin.com/in/vincenttoaso/) | [Email](vincent.toaso@gmail.com)
