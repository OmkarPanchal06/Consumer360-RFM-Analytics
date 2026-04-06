#  Consumer360: Project Documentation & Business Logic

Welcome to the **Consumer360-RFM-Analytics** suite. This document provides a detailed, beginner-friendly walkthrough of the architecture, algorithms, and mathematical formulas used to transform raw data into high-value business intelligence.

---

## 1. What is this Project?
In simple terms, this project is a **Customer Intelligence Engine**. It takes thousands of raw sales transactions from a database and answers three critical questions for a business:
1. **Who are my best customers?** (Segmentation)
2. **How are they behaving?** (RFM Analysis)
3. **How much money will they bring in the next 5 years?** (CLV Prediction)

---

## 2. The Data Journey (ETL Pipeline)
Data follows a professional **ETL (Extract, Transform, Load)** process:
1. **Extract**: Data is pulled from a **Local SQL Server** using Python.
2. **Transform**: Python applies mathematical "Algorithms" (RFM & CLV) to create new insights.
3. **Load**: The final results are exported to a **Premium HTML Dashboard** and a **Power BI Excel Registry**.

---

## 3. The RFM Algorithm (Behavioral Scoring)
The core "Algorithm" used for segmentation is **RFM Analysis**. It scores customers based on three variables:

### A. The Three Pillars
*   **Recency (R)**: How many days ago was their last purchase? (Newer is better)
*   **Frequency (F)**: How many times have they bought from us? (More is better)
*   **Monetary (M)**: How much total money have they spent? (Higher is better)

### B. The Scoring Formula (Quintile Method)
We don't just use the raw numbers; we rank customers relative to each other using **Quintiles** (Top 20%, Next 20%, etc.).

> [!NOTE]
> **Quintile Scoring**: We divide all customers into 5 equal groups. The top 20% get a score of **5**, and the bottom 20% get a score of **1**.

### C. The Weighted RFM Index
Not all "pillars" are equal. To get a single "Customer Quality" score, we use this formula:
$$RFM\_Score = \frac{(R \times 1.0) + (F \times 1.2) + (M \times 1.5)}{3.7}$$

*   **Why the weights?** We prioritize **Monetary (1.5)** and **Frequency (1.2)** because they represent actual cash-flow and loyalty more than just Recency (1.0).

---

## 4. Customer Segmentation Logic
Once we have the scores, we group customers into "Segments" using a rule-based algorithm:

| Segment | Logic / Rules | Business Action |
| :--- | :--- | :--- |
| **Champions** | RFM Score $\ge 4.5$ | Reward them; they are your fans. |
| **Loyal Customers** | Frequency $\ge 4$ AND Monetary $\ge 4$ | Upsell high-value products. |
| **Potential Loyalists** | Recently active AND decent Frequency | Offer a loyalty program. |
| **At Risk** | Low Recency, but high F and M | Send "We Miss You" discounts. |
| **Hibernating** | Low scores across all three areas | Don't spend much on marketing here. |

---

## 5. CLV Prediction (Predictive Algorithm)
**Customer Lifetime Value (CLV)** tries to predict the future. We use a **Deterministic Growth Model** to estimate value over the next 5 years.

### The Formula:
1.  **Calculate Annual Revenue**:
    $$Annual\_Revenue = Avg\_Order\_Value \times \frac{Purchase\_Count}{\frac{Days\_Active}{365} + 1}$$
2.  **Predict 5-Year Value**:
    $$CLV = Annual\_Revenue \times 5 \text{ (Projected Years)}$$

> [!TIP]
> This formula helps businesses decide how much they can afford to spend on "Acquiring" a new customer. If the CLV is $5,000, spending $100 on an ad is a great deal!

---

## 6. The Premium Technology Stack
*   **Database**: SQL Server (Star Schema architecture).
*   **Logic**: Python (Pandas for data math).
*   **Visuals**: Plotly & CSS (Glassmorphism design).
*   **Deployment**: GitHub Pages (Live web hosting).

---

### Summary for Beginners
By combining **History** (RFM) with **Future** (CLV), this project allows a business to stop "guessing" and start "knowing" which customers drive their profit.
