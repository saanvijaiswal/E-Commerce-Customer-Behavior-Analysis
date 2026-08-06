# E-Commerce-Customer-Behavior-Analysis

A revenue-concentration and retention analysis built on 1 year of UK e-commerce transaction data (UCI Online Retail dataset), identifying where revenue is concentrated, which customers are lapsing, and how much of that lapsed revenue is recoverable.

## Key Findings
- Revenue is heavily concentrated in a single segment. "Champions" -> customers who purchase both recently and repeatedly - account for $2.65M of $3.87M total revenue (~68%), despite being a minority of the customer base.
- ~$0.85M in revenue sits with At Risk and Lost customers ($0.53M + $0.32M combined) -> customers who were once active but have gone quiet. This is a sized, actionable target for a retention campaign, not a vague "win back lapsed users" instruction.
- Month-1 retention is just 13.65% - the large majority of new customers never return after their first purchase. Along with the concentration above, this points to acquisition-without-retention as the core structural pattern in the underlying business.
- Repeat Purchase Rate sits at 51.18% - roughly half of all customers purchase more than once, which puts the low Month-1 figure in context: retention risk is front-loaded in the first purchase cycle rather than spread evenly over time.

## What's in the Dashboard
- **KPI headline metrics:** Total Revenue, Unique Customers, Avg Order Value, Repeat Purchase Rate, Month-1 Retention Rate
- **RFM-based customer segmentation** (Champions / At Risk / Lost / New Customers): built on custom DAX scoring against Recency and Frequency
- **Cohort-based acquisition and retention analysis:** with conditional-formatted visuals highlighting concentration and drop-off patterns month over month
- **Revenue trend analysis:** current vs. prior-month comparison
- **Country and product filtering:** plus a rolling date-range timeline slicer

Dashboard Preview
<img width="1220" height="687" alt="image" src="https://github.com/user-attachments/assets/0989b183-0169-4026-aa05-31a156a87010" />

Dashboard Preview After using Slicers (Multiple products selected)
<img width="1216" height="687" alt="image" src="https://github.com/user-attachments/assets/9ba8279a-81e4-42d9-90f3-5d5d760e043a" />


## Methodology
**RFM segmentation:** Customers scored on Recency (days since last purchase) and Frequency (distinct orders placed), ranked against the full customer base, then bucketed into 4 business-readable segments using threshold-based rules rather than opaque quintile labels.
**Cohort analysis:** customers grouped by month of first purchase (Customer Cohort Month); retention tracked by what share of each cohort remains active in each subsequent month (Cohort Index).
**All measures built in DAX** inside Power BI, with the transaction table related to a dedicated date table for accurate time-intelligence calculations (month-over-month comparisons, cohort indexing).
**Custom visual theme** (color palette + typography) applied across all visuals, replacing Power BI's default styling.

## How to Reproduce
1. Download Customer-Behavior-Analysis.pbix and open it in Power BI Desktop (free).
2. Source dataset: UCI Online Retail Dataset.
3. All DAX measures and the data model are contained within the file — no external setup required beyond opening it.

## Project Evolution

This project has been iterated on beyond its original version - earlier work focused on descriptive KPIs and trend charts. This version adds:

- **RFM-based customer segmentation** to identify which customers are worth protecting vs. recovering, not just how many customers exist
- **Cohort retention analysis** to quantify how quickly new customers churn, rather than only reporting an aggregate repeat-purchase rate
- **Revenue concentration analysis** (segment-level revenue share) to surface where the business is actually dependent for revenue, instead of a single top-line total
- A **custom visual design system** (color theme, typography, conditional formatting) replacing default Power BI styling
- **DAX Measures**

Full iteration history is visible in this repo's commit log.
Custom visual theme (color palette + typography) applied across all visuals, replacing Power BI's default styling.

