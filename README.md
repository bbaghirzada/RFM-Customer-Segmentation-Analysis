# Customer Lifetime Value (CLV) Analysis Using Cohorts

## Overview

This project focuses on calculating Customer Lifetime Value (CLV) using cohort analysis, which provides a more reliable and actionable approach. The analysis is based on user behavior data from an e-commerce platform, specifically using the `turing college provided database` table. The goal is to provide insights into customer retention, revenue growth, and future revenue predictions based on historical data.

## Key Features

- **Cohort Analysis**: The analysis is divided into weekly cohorts based on the first visit (registration date) of users. This allows for a detailed examination of customer behavior over time.
- **Cumulative Revenue Calculation**: The project calculates cumulative revenue for each cohort, providing insights into how revenue grows over weeks after registration.
- **Revenue Prediction**: Using historical data, the project predicts future revenue for each cohort up to 12 weeks after registration.
- **RFM Segmentation**: The project also includes RFM (Recency, Frequency, Monetary) segmentation to categorize customers based on their purchasing behavior.

## Methodology

1. **Cohort Creation**: 
   - Users are grouped into weekly cohorts based on their first visit (registration date).
   - Revenue is calculated for each cohort over 12 weeks.

2. **Cumulative Revenue Calculation**:
   - Cumulative revenue is calculated by summing up the revenue for each week and adding it to the previous week's total.
   - This provides a clear picture of revenue growth over time for each cohort.

3. **Revenue Prediction**:
   - Future revenue is predicted using the cumulative growth percentage calculated from historical data.
   - The prediction is made for up to 12 weeks after registration.

4. **RFM Segmentation**:
   - Customers are segmented based on their Recency, Frequency, and Monetary scores.
   - This segmentation helps in identifying high-value customers, at-risk customers, and potential loyalists.

## SQL Queries

The project involves several SQL queries to extract, transform, and analyze the data. Below is a summary of the key queries used:

### 1. RFM Scores and Customer Segmentation Query

```sql
WITH MaxDate AS (
    SELECT MAX(CAST(InvoiceDate AS DATE)) + 1 AS reference_date
    FROM `turing_data_analytics.rfm`
),

RFM_Calculations AS ( 
    SELECT 
        CustomerID,
        Country,
        CAST(MAX(InvoiceDate) AS DATE) AS last_purchase_date,
        COUNT(DISTINCT InvoiceNo) AS frequency,
        SUM(UnitPrice * Quantity) AS monetary,
        DATE_DIFF((SELECT reference_date FROM MaxDate), CAST(MAX(InvoiceDate) AS DATE), DAY) AS recency  
    FROM `turing_data_analytics.rfm`
    WHERE InvoiceDate BETWEEN '2010-12-01' AND '2011-12-01'
        AND CustomerID IS NOT NULL
        AND Description IS NOT NULL
    GROUP BY CustomerID, Country
),

RFM_Quantiles AS (
    SELECT
        APPROX_QUANTILES(monetary, 4)[OFFSET(1)] AS m25,  -- 25th percentile
        APPROX_QUANTILES(monetary, 4)[OFFSET(2)] AS m50,  -- 50th percentile
        APPROX_QUANTILES(monetary, 4)[OFFSET(3)] AS m75,  -- 75th percentile
        APPROX_QUANTILES(frequency, 4)[OFFSET(1)] AS f25,
        APPROX_QUANTILES(frequency, 4)[OFFSET(2)] AS f50,
        APPROX_QUANTILES(frequency, 4)[OFFSET(3)] AS f75,
        APPROX_QUANTILES(recency, 4)[OFFSET(1)] AS r25,
        APPROX_QUANTILES(recency, 4)[OFFSET(2)] AS r50,
        APPROX_QUANTILES(recency, 4)[OFFSET(3)] AS r75
    FROM RFM_Calculations
),

RFM_Scores AS (
    SELECT 
        a.CustomerID,
        a.Country,
        a.recency,
        a.frequency,
        a.monetary,
        CASE 
            WHEN a.monetary <= b.m25 THEN 1
            WHEN a.monetary <= b.m50 THEN 2
            WHEN a.monetary <= b.m75 THEN 3
            ELSE 4
        END AS m_score,
        CASE 
            WHEN a.frequency <= b.f25 THEN 1
            WHEN a.frequency <= b.f50 THEN 2
            WHEN a.frequency <= b.f75 THEN 3
            ELSE 4
        END AS f_score,
        CASE 
            WHEN a.recency <= b.r25 THEN 4  -- More recent customers get higher scores
            WHEN a.recency <= b.r50 THEN 3
            WHEN a.recency <= b.r75 THEN 2
            ELSE 1
        END AS r_score
    FROM RFM_Calculations a
    CROSS JOIN RFM_Quantiles b
)

SELECT 
    CustomerID,
    Country,
    recency,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score,
    (r_score * 100) + (f_score * 10) + m_score AS rfm_score,
    CASE
        WHEN r_score = 4 AND f_score = 4 AND m_score = 4 THEN 'Champions'
        WHEN r_score = 4 AND f_score >= 3 AND m_score >= 3 THEN 'Loyal Customers'
        WHEN r_score >= 3 AND f_score >= 2 AND m_score >= 2 THEN 'Potential Loyalists'
        WHEN r_score = 4 AND f_score = 1 THEN 'Recent Customers'
        WHEN r_score = 3 AND f_score = 1 THEN 'Promising'
        WHEN r_score <= 2 AND f_score >= 3 THEN 'Customers Needing Attention'
        WHEN r_score = 1 AND f_score >= 1 THEN 'At Risk'
        ELSE 'Other'
    END AS rfm_segment
FROM RFM_Scores
```

### 2. Revenue Calculation

```sql
-- Query to calculate cumulative revenue by cohorts
WITH weekly_cohorts AS (
    SELECT
        user_pseudo_id,
        DATE_TRUNC(DATE(TIMESTAMP_MICROS(MIN(event_timestamp))), WEEK) AS reg_date
    FROM
        `turing_data_analytics.raw_events`
    WHERE
        DATE_TRUNC(DATE(TIMESTAMP_MICROS(event_timestamp)), WEEK) <= '2021-01-24'
    GROUP BY
        user_pseudo_id
),
weekly_revenue AS (
    SELECT
        user_pseudo_id,
        DATE_TRUNC(DATE(TIMESTAMP_MICROS(event_timestamp)), WEEK) AS p_date,
        purchase_revenue_in_usd AS p_revenue
    FROM
        `turing_data_analytics.raw_events`
)
SELECT
    wc.reg_date,
    SUM(IF(wr.p_date = wc.reg_date, wr.p_revenue, 0)) / COUNT(DISTINCT wc.user_pseudo_id) AS week_0,
    SUM(IF(wr.p_date = TIMESTAMP_ADD(wc.reg_date, INTERVAL 1 WEEK), wr.p_revenue, 0)) / COUNT(DISTINCT wc.user_pseudo_id) AS week_1,
    SUM(IF(wr.p_date = TIMESTAMP_ADD(wc.reg_date, INTERVAL 2 WEEK), wr.p_revenue, 0)) / COUNT(DISTINCT wc.user_pseudo_id) AS week_2,
    SUM(IF(wr.p_date = TIMESTAMP_ADD(wc.reg_date, INTERVAL 3 WEEK), wr.p_revenue, 0)) / COUNT(DISTINCT wc.user_pseudo_id) AS week_3,
    SUM(IF(wr.p_date = TIMESTAMP_ADD(wc.reg_date, INTERVAL 4 WEEK), wr.p_revenue, 0)) / COUNT(DISTINCT wc.user_pseudo_id) AS week_4,
    SUM(IF(wr.p_date = TIMESTAMP_ADD(wc.reg_date, INTERVAL 5 WEEK), wr.p_revenue, 0)) / COUNT(DISTINCT wc.user_pseudo_id) AS week_5,
    SUM(IF(wr.p_date = TIMESTAMP_ADD(wc.reg_date, INTERVAL 6 WEEK), wr.p_revenue, 0)) / COUNT(DISTINCT wc.user_pseudo_id) AS week_6,
    SUM(IF(wr.p_date = TIMESTAMP_ADD(wc.reg_date, INTERVAL 7 WEEK), wr.p_revenue, 0)) / COUNT(DISTINCT wc.user_pseudo_id) AS week_7,
    SUM(IF(wr.p_date = TIMESTAMP_ADD(wc.reg_date, INTERVAL 8 WEEK), wr.p_revenue, 0)) / COUNT(DISTINCT wc.user_pseudo_id) AS week_8,
    SUM(IF(wr.p_date = TIMESTAMP_ADD(wc.reg_date, INTERVAL 9 WEEK), wr.p_revenue, 0)) / COUNT(DISTINCT wc.user_pseudo_id) AS week_9,
    SUM(IF(wr.p_date = TIMESTAMP_ADD(wc.reg_date, INTERVAL 10 WEEK), wr.p_revenue, 0)) / COUNT(DISTINCT wc.user_pseudo_id) AS week_10,
    SUM(IF(wr.p_date = TIMESTAMP_ADD(wc.reg_date, INTERVAL 11 WEEK), wr.p_revenue, 0)) / COUNT(DISTINCT wc.user_pseudo_id) AS week_11,
    SUM(IF(wr.p_date = TIMESTAMP_ADD(wc.reg_date, INTERVAL 12 WEEK), wr.p_revenue, 0)) / COUNT(DISTINCT wc.user_pseudo_id) AS week_12
FROM
    weekly_revenue wr
JOIN
    weekly_cohorts wc
ON
    wr.user_pseudo_id = wc.user_pseudo_id
GROUP BY
    wc.reg_date
ORDER BY
    wc.reg_date;

```
---

## Results

The analysis provides the following key insights:

1. **Weekly Average Revenue by Cohorts**: This chart shows the average revenue generated by each cohort over 12 weeks.
2. **Cumulative Revenue by Cohorts**: This chart shows the cumulative revenue growth for each cohort over time.
3. **Revenue Prediction by Cohorts**: This chart predicts future revenue for each cohort based on historical growth rates.
