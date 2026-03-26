-- ============================================================
-- CREATE DATABASE TABLES
-- ============================================================

USE Consumer360_DB;
GO

-- Table 1: Dim_Date (Calendar)
CREATE TABLE Dim_Date (
    DateKey INT PRIMARY KEY,
    CalendarDate DATE UNIQUE NOT NULL,
    Year INT NOT NULL,
    Quarter INT NOT NULL,
    Month INT NOT NULL,
    Day INT NOT NULL,
    WeekOfYear INT NOT NULL,
    DayOfWeek INT NOT NULL,
    DayName VARCHAR(20) NOT NULL,
    MonthName VARCHAR(20) NOT NULL,
    IsWeekend BIT NOT NULL,
    IsHoliday BIT NOT NULL DEFAULT 0
);

PRINT '✓ Dim_Date table created';

-- Table 2: Dim_Region (Geographic Dimension)
CREATE TABLE Dim_Region (
    RegionKey INT PRIMARY KEY IDENTITY(1,1),
    RegionID NVARCHAR(50) UNIQUE NOT NULL,
    RegionName NVARCHAR(100) NOT NULL,
    Country NVARCHAR(100) NOT NULL,
    State NVARCHAR(100),
    City NVARCHAR(100),
    IsActive BIT NOT NULL DEFAULT 1
);

PRINT '✓ Dim_Region table created';

-- Table 3: Dim_Product (Product Dimension)
CREATE TABLE Dim_Product (
    ProductKey INT PRIMARY KEY IDENTITY(1,1),
    ProductID NVARCHAR(50) UNIQUE NOT NULL,
    ProductName NVARCHAR(200) NOT NULL,
    Category NVARCHAR(100) NOT NULL,
    SubCategory NVARCHAR(100),
    UnitPrice DECIMAL(10, 2) NOT NULL CHECK (UnitPrice >= 0),
    Cost DECIMAL(10, 2),
    ProductStatus NVARCHAR(20) DEFAULT 'Active',
    IsActive BIT NOT NULL DEFAULT 1
);

PRINT '✓ Dim_Product table created';

-- Table 4: Dim_Customer (Customer Dimension)
CREATE TABLE Dim_Customer (
    CustomerKey INT PRIMARY KEY IDENTITY(1,1),
    CustomerID NVARCHAR(50) UNIQUE NOT NULL,
    CustomerName NVARCHAR(100) NOT NULL,
    Email NVARCHAR(100),
    Phone NVARCHAR(20),
    RegistrationDate DATE NOT NULL,
    LifecycleStatus NVARCHAR(20) DEFAULT 'Active',
    IsCurrentRecord BIT NOT NULL DEFAULT 1,
    CreatedDate DATETIME NOT NULL DEFAULT GETDATE()
);

PRINT '✓ Dim_Customer table created';

-- Table 5: Fact_Sales (Main Fact Table)
CREATE TABLE Fact_Sales (
    SalesID BIGINT PRIMARY KEY IDENTITY(1000001,1),
    CustomerKey INT NOT NULL,
    ProductKey INT NOT NULL,
    DateKey INT NOT NULL,
    RegionKey INT NOT NULL,
    OrderNumber NVARCHAR(50) NOT NULL,
    OrderLineNumber INT NOT NULL,
    TransactionDate DATETIME NOT NULL,
    Quantity INT NOT NULL CHECK (Quantity > 0),
    UnitPrice DECIMAL(10, 2) NOT NULL,
    SalesAmount DECIMAL(15, 2) NOT NULL,
    DiscountAmount DECIMAL(10, 2) DEFAULT 0,
    TaxAmount DECIMAL(10, 2) DEFAULT 0,
    NetSalesAmount DECIMAL(15, 2),
    ReturnFlag CHAR(1) DEFAULT 'N',
    CreatedDate DATETIME NOT NULL DEFAULT GETDATE(),
    
    FOREIGN KEY (CustomerKey) REFERENCES Dim_Customer(CustomerKey),
    FOREIGN KEY (ProductKey) REFERENCES Dim_Product(ProductKey),
    FOREIGN KEY (DateKey) REFERENCES Dim_Date(DateKey),
    FOREIGN KEY (RegionKey) REFERENCES Dim_Region(RegionKey)
);

PRINT '✓ Fact_Sales table created';

-- CREATE INDEXES
CREATE NONCLUSTERED INDEX IX_Fact_Sales_CustomerKey ON Fact_Sales(CustomerKey);
CREATE NONCLUSTERED INDEX IX_Fact_Sales_ProductKey ON Fact_Sales(ProductKey);
CREATE NONCLUSTERED INDEX IX_Fact_Sales_DateKey ON Fact_Sales(DateKey);
CREATE NONCLUSTERED INDEX IX_Dim_Customer_RegistrationDate ON Dim_Customer(RegistrationDate);

PRINT '✓ All indexes created';
PRINT '✓ Schema setup complete!';