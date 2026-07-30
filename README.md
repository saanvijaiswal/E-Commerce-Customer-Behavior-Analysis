# E-Commerce-Customer-Behavior-Analysis

This project analyzes an e-commerce dataset to uncover customer buying patterns, sales trends, and repeat purchase behavior.

I built the end-to-end pipeline using:
- **Python** for data cleaning and RFM segmentation
- **SQL** for structured querying
- **Power BI** for interactive visualization and insights


## Dashboard Preview

**Dashboard after Sorting by Country**
<img width="1432" height="812" alt="image" src="https://github.com/user-attachments/assets/c073eb79-c11f-43c7-9cb1-ebcc74691120" />


**Customer Repeat Purchase Behavior**
<img width="473" height="477" alt="image" src="https://github.com/user-attachments/assets/4b86d187-0df4-4a11-ae78-3316cedb834f" />


- **France, UK, and Germany** drive 70% of total sales.
- Peak order hours are between **10 AM – 1 PM**.
- ~58% of customers are **one-time buyers**, 10% are **loyal buyers** (6+ orders).
- High revenue correlation between **order frequency** and **average spend**.

## 🧮 Data Process
1. **Python (Preprocessing)**  
   - Cleaned `InvoiceDate`, removed nulls & duplicates.  
   - Created `TotalAmount = Quantity × UnitPrice`.  
   - Exported to `ecommerce_cleaned.csv`.

2. **SQL (Analysis)**  
   - Queried top products, countries, and monthly sales trends.  

3. **Power BI (Visualization)**  
   - KPIs: Total Revenue, Orders, Unique Customers.  
   - Trend Charts: Monthly & Daily Sales.  
   - Segmentation: RFM & Repeat Buyer analysis.


