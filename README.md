# E-Commerce-Customer-Behavior-Analysis

A revenue-concentration and retention analysis built on 1 year of UK e-commerce transaction data (UCI Online Retail dataset), identifying where revenue is concentrated, which customers are lapsing, and how much of that lapsed revenue is recoverable.

## Business Question

**Which customer segments and product categories should the business prioritize to grow repeat revenue, and where is demand concentrated?**
Acquiring a new customer costs more than retaining one - so understanding who buys once vs. who comes back, and what drives that difference, directly informs where marketing and retention budget should go.

## Key Findings
- Revenue is heavily concentrated in a single segment. "Champions" -> customers who purchase both recently and repeatedly - account for $2.65M of $3.87M total revenue (~68%), despite being a minority of the customer base.
- ~$0.85M in revenue sits with At Risk and Lost customers ($0.53M + $0.32M combined) -> customers who were once active but have gone quiet. This is a sized, actionable target for a retention campaign, not a vague "win back lapsed users" instruction.
- Month-1 retention is just 13.65% - the large majority of new customers never return after their first purchase. Along with the concentration above, this points to acquisition-without-retention as the core structural pattern in the underlying business.
- Repeat Purchase Rate sits at 51.18% - roughly half of all customers purchase more than once, which puts the low Month-1 figure in context: retention risk is front-loaded in the first purchase cycle rather than spread evenly over time.

## What's in the Dashboard

The dashboard is organized into 3 pages - **Executive Summary**, **Customer Segmentation**, and **Cohort Retention** — inside a custom sidebar-navigation frame, with consistent country/product filtering on every page.

**Executive Summary**
* KPI headline metrics with month-over-month trend indicators: Total Revenue, Unique Customers, Avg Order Value, Repeat Purchase Rate, Month-1 Retention Rate
* Revenue trend analysis: current vs. prior-month comparison
* Revenue concentration by customer segment, with cumulative share tracked across segments
* Segment revenue rank over time (ribbon chart), showing how each segment's relative revenue contribution shifts month to month
* Revenue and customer-count breakdowns by segment (donut and bar)

**Customer Segmentation**
* RFM-based customer segmentation (Champions / At Risk / Lost / New Customers): built on custom DAX scoring against Recency and Frequency
* Customer Lifetime Value by segment, alongside a customer-level detail table with inline data bars and colored segment badges
* Customer purchase progression funnel, showing drop-off from first purchase through repeat-purchase milestones
* Revenue growth bridge by segment (waterfall), and order-value distribution (histogram)
* RFM scatter plot (Recency vs. Frequency, sized by Monetary value, colored by segment) and supporting headline callouts (e.g. revenue from top 20% of customers)

**Cohort Retention**
* Cohort-based acquisition and retention analysis, with conditional-formatted visuals highlighting concentration and drop-off patterns month over month
* Cohort retention heatmap (matrix with gradient conditional formatting) alongside the retention trend line
* Customer vs. revenue retention comparison by cohort index, plus Month-1 retention and best-performing cohort callouts

**Filtering**
* Country and product filtering on every page, plus a rolling date-range timeline slicer

## Dashboard Preview
Executive Summary | Customer Segmentation | Cohort Retention

<img width="960" height="537" alt="Executive summary" src="https://github.com/user-attachments/assets/a6e077d2-3b5a-4d9a-b1e5-6396277a90f6" />
<img width="961" height="552" alt="Customer Segmentation" src="https://github.com/user-attachments/assets/be29962c-6b45-4ff5-8b68-06e4f1ba7d67" />
<img width="952" height="522" alt="Cohort Retention" src="https://github.com/user-attachments/assets/ebd78e4d-0ccd-461c-850d-8599c5f68b8a" />

## Methodology

**RFM segmentation:** Customers scored on Recency (days since last purchase) and Frequency (distinct orders placed), ranked against the full customer base, then bucketed into 4 business-readable segments using threshold-based rules rather than opaque quintile labels.

**Cohort analysis:** customers grouped by month of first purchase (Customer Cohort Month); retention tracked by what share of each cohort remains active in each subsequent month (Cohort Index), measured both by customer count and by revenue retained.

All measures built in DAX inside Power BI, with the transaction table related to a dedicated date table for accurate time-intelligence calculations (month-over-month comparisons, cohort indexing).

Custom visual theme (color palette + typography) applied across all visuals, replacing Power BI's default styling, within a custom sidebar-navigation frame built entirely from native Power BI shapes and visuals.

## How to Reproduce
1. Download Customer-Behavior-Analysis.pbix and open it in Power BI Desktop (free).
2. Source dataset: UCI Online Retail Dataset.
3. All DAX measures and the data model are contained within the file — no external setup required beyond opening it.

## Project Evolution

This project has been iterated on beyond its original version - earlier work focused on descriptive KPIs and trend charts. This version adds:

- **RFM-based customer segmentation** to identify which customers are worth protecting vs. recovering, not just how many customers exist
- **Cohort retention analysis** to quantify how quickly new customers churn, rather than only reporting an aggregate repeat-purchase rate
- **Revenue concentration analysis** (segment-level revenue share) to surface where the business is actually dependent for revenue, instead of a single top-line total
- A **custom visual design system** (color theme, conditional formatting) replacing default Power BI styling
- **DAX Measures**

Full iteration history is visible in this repo's commit log.
Custom visual theme (color palette + typography) applied across all visuals, replacing Power BI's default styling.

