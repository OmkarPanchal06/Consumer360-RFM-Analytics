USE Consumer360_DB;
GO

-- INSERT REGIONS
INSERT INTO Dim_Region (RegionID, RegionName, Country, State, City, IsActive)
VALUES
    ('R001', 'California West', 'USA', 'California', 'Los Angeles', 1),
    ('R002', 'New York East', 'USA', 'New York', 'New York', 1),
    ('R003', 'Texas Central', 'USA', 'Texas', 'Houston', 1),
    ('R004', 'UK London', 'UK', 'England', 'London', 1);

PRINT '✓ Regions inserted';

-- INSERT PRODUCTS
INSERT INTO Dim_Product (ProductID, ProductName, Category, SubCategory, UnitPrice, Cost, IsActive)
VALUES
    ('P001', 'iPhone 14', 'Electronics', 'Phones', 999.99, 400.00, 1),
    ('P002', 'Samsung Galaxy', 'Electronics', 'Phones', 899.99, 350.00, 1),
    ('P003', 'MacBook Pro', 'Electronics', 'Laptops', 1999.99, 800.00, 1),
    ('P004', 'Diapers Pack', 'Baby Care', 'Diapers', 29.99, 10.00, 1),
    ('P005', 'Beer 6-Pack', 'Beverages', 'Beer', 12.99, 4.00, 1),
    ('P006', 'Coffee Beans', 'Groceries', 'Coffee', 19.99, 5.00, 1);

PRINT '✓ Products inserted';

-- INSERT CUSTOMERS
INSERT INTO Dim_Customer (CustomerID, CustomerName, Email, Phone, RegistrationDate, IsCurrentRecord)
VALUES
    ('C001', 'John Smith', 'john@email.com', '555-0001', '2021-01-15', 1),
    ('C002', 'Sarah Johnson', 'sarah@email.com', '555-0002', '2021-06-22', 1),
    ('C003', 'Mike Wilson', 'mike@email.com', '555-0003', '2022-03-10', 1),
    ('C004', 'Emma Brown', 'emma@email.com', '555-0004', '2022-11-05', 1),
    ('C005', 'David Lee', 'david@email.com', '555-0005', '2023-02-14', 1);

PRINT '✓ Customers inserted';

-- INSERT SALES TRANSACTIONS
INSERT INTO Fact_Sales (
    CustomerKey, ProductKey, DateKey, RegionKey, OrderNumber, OrderLineNumber,
    TransactionDate, Quantity, UnitPrice, SalesAmount, DiscountAmount, TaxAmount, 
    NetSalesAmount, ReturnFlag
)
VALUES
    -- John Smith (Champion Customer)
    (1, 1, 20260319, 1, 'ORD001', 1, GETDATE()-5, 1, 999.99, 999.99, 0, 80, 999.99, 'N'),
    (1, 3, 20260309, 1, 'ORD002', 1, GETDATE()-15, 1, 1999.99, 1999.99, 200, 144, 1799.99, 'N'),
    
    -- Sarah Johnson (Loyal Customer)
    (2, 2, 20260314, 2, 'ORD003', 1, GETDATE()-10, 1, 899.99, 899.99, 90, 64, 809.99, 'N'),
    (2, 5, 20260208, 2, 'ORD004', 1, GETDATE()-45, 2, 12.99, 25.98, 0, 2, 25.98, 'N'),
    
    -- Mike Wilson (At Risk)
    (3, 1, 20250905, 3, 'ORD005', 1, GETDATE()-180, 1, 999.99, 999.99, 0, 80, 999.99, 'N'),
    
    -- Emma Brown (New Customer)
    (4, 6, 20260322, 4, 'ORD006', 1, GETDATE()-2, 1, 19.99, 19.99, 0, 1.6, 19.99, 'N'),
    
    -- David Lee (Hibernating)
    (5, 4, 20250324, 1, 'ORD007', 1, GETDATE()-365, 2, 29.99, 59.98, 0, 4.8, 59.98, 'N');

PRINT '✓ Sales transactions inserted';

-- VERIFY DATA
SELECT 'Dim_Region' AS Table_Name, COUNT(*) AS Row_Count FROM Dim_Region
UNION ALL
SELECT 'Dim_Product', COUNT(*) FROM Dim_Product
UNION ALL
SELECT 'Dim_Customer', COUNT(*) FROM Dim_Customer
UNION ALL
SELECT 'Fact_Sales', COUNT(*) FROM Fact_Sales;