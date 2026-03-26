USE Consumer360_DB;
GO

CREATE VIEW vw_Customer360_SingleView AS
WITH CustomerMetrics AS (
    SELECT
        c.CustomerKey,
        c.CustomerID,
        c.CustomerName,
        c.Email,
        c.Phone,
        c.RegistrationDate,
        
        -- Recency: Days since last purchase
        DATEDIFF(DAY, MAX(fs.TransactionDate), CAST(GETDATE() AS DATE)) AS RecencyDays,
        
        -- Frequency: Total transactions
        COUNT(DISTINCT fs.SalesID) AS TransactionCount,
        
        -- Monetary: Total spend
        SUM(CASE WHEN fs.ReturnFlag = 'N' THEN fs.NetSalesAmount ELSE 0 END) AS TotalSpend,
        
        -- Other metrics
        SUM(CASE WHEN fs.ReturnFlag = 'N' THEN 1 ELSE 0 END) AS PurchaseCount,
        SUM(CASE WHEN fs.ReturnFlag = 'Y' THEN 1 ELSE 0 END) AS ReturnCount,
        
        CAST(SUM(CASE WHEN fs.ReturnFlag = 'N' THEN fs.NetSalesAmount ELSE 0 END) AS DECIMAL(15,2)) / 
            NULLIF(SUM(CASE WHEN fs.ReturnFlag = 'N' THEN 1 ELSE 0 END), 0) AS AvgOrderValue,
        
        DATEDIFF(DAY, c.RegistrationDate, CAST(GETDATE() AS DATE)) AS CustomerTenureDays,
        
        YEAR(c.RegistrationDate) AS CohortYear,
        DATEPART(QUARTER, c.RegistrationDate) AS CohortQuarter,
        MONTH(c.RegistrationDate) AS CohortMonth,
        
        COUNT(DISTINCT DATEPART(MONTH, fs.TransactionDate)) AS MonthsActive,
        
        MAX(r.RegionName) AS PrimaryRegion,
        MAX(r.Country) AS Country,
        MAX(r.State) AS State,
        
        MAX(fs.TransactionDate) AS LastPurchaseDate,
        COUNT(DISTINCT p.Category) AS CategoriesPurchased
        
    FROM Dim_Customer c
    LEFT JOIN Fact_Sales fs ON c.CustomerKey = fs.CustomerKey AND fs.ReturnFlag = 'N'
    LEFT JOIN Dim_Region r ON fs.RegionKey = r.RegionKey
    LEFT JOIN Dim_Product p ON fs.ProductKey = p.ProductKey
    WHERE c.IsCurrentRecord = 1
    GROUP BY c.CustomerKey, c.CustomerID, c.CustomerName, c.Email, c.Phone, c.RegistrationDate
)
SELECT
    *,
    CASE
        WHEN RecencyDays IS NULL THEN 'Never Purchased'
        WHEN RecencyDays <= 30 THEN 'Very Recent'
        WHEN RecencyDays <= 90 THEN 'Recent'
        WHEN RecencyDays <= 180 THEN 'Moderate'
        ELSE 'Distant'
    END AS RecencyCategory
FROM CustomerMetrics
WHERE TotalSpend > 0 OR TransactionCount > 0;

GO

PRINT '✓ Customer 360 view created!';