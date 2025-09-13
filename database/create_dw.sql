-- Tables de Dimension
CREATE TABLE DimProduct (
    ProductID INT IDENTITY(1,1) PRIMARY KEY,
    ProductName NVARCHAR(255) NOT NULL,
    CategoryID INT,
    Price DECIMAL(10,2) NOT NULL,
    Cost DECIMAL(10,2),
    UOM NVARCHAR(50),
    CreatedAt DATETIME DEFAULT GETDATE(),
    UpdatedAt DATETIME DEFAULT GETDATE()
);

CREATE TABLE DimProductCategory (
    CategoryID INT IDENTITY(1,1) PRIMARY KEY,
    CategoryName NVARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE DimCustomer (
    CustomerID INT IDENTITY(1,1) PRIMARY KEY,
    CustomerName NVARCHAR(255) NOT NULL,
    Email NVARCHAR(255) UNIQUE,
    Phone NVARCHAR(20),
    Address NVARCHAR(MAX)
);

CREATE TABLE DimSupplier (
    SupplierID INT IDENTITY(1,1) PRIMARY KEY,
    SupplierName NVARCHAR(255) NOT NULL,
    ContactEmail NVARCHAR(255),
    ContactPhone NVARCHAR(20),
    Address NVARCHAR(MAX)
);

CREATE TABLE DimWarehouse (
    WarehouseID INT IDENTITY(1,1) PRIMARY KEY,
    WarehouseName NVARCHAR(255) NOT NULL,
    Location NVARCHAR(255)
);

CREATE TABLE DimDate (
    DateID INT IDENTITY(1,1) PRIMARY KEY,
    Date DATE NOT NULL,
    Day INT NOT NULL,
    Month INT NOT NULL,
    Year INT NOT NULL,
    Quarter INT NOT NULL,
    Weekday INT NOT NULL
);

-- Tables de Faits
CREATE TABLE FactSales (
    SalesOrderID INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID INT REFERENCES DimCustomer(CustomerID),
    ProductID INT REFERENCES DimProduct(ProductID),
    OrderDate INT REFERENCES DimDate(DateID),
    Quantity INT NOT NULL,
    UnitPrice DECIMAL(10,2) NOT NULL,
    TotalPrice DECIMAL(10,2) NOT NULL
);

CREATE TABLE FactPurchase (
    PurchaseOrderID INT IDENTITY(1,1) PRIMARY KEY,
    SupplierID INT REFERENCES DimSupplier(SupplierID),
    ProductID INT REFERENCES DimProduct(ProductID),
    OrderDate INT REFERENCES DimDate(DateID),
    Quantity INT NOT NULL,
    UnitCost DECIMAL(10,2) NOT NULL,
    TotalCost DECIMAL(10,2) NOT NULL
);

CREATE TABLE FactStock (
    StockID INT IDENTITY(1,1) PRIMARY KEY,
    ProductID INT REFERENCES DimProduct(ProductID),
    WarehouseID INT REFERENCES DimWarehouse(WarehouseID),
    Quantity INT NOT NULL,
    MinQuantity INT DEFAULT 0,
    MaxQuantity INT DEFAULT 0,
    UpdatedAt DATETIME DEFAULT GETDATE()
);

CREATE TABLE FactStockMovement (
    MovementID INT IDENTITY(1,1) PRIMARY KEY,
    ProductID INT REFERENCES DimProduct(ProductID),
    WarehouseID INT REFERENCES DimWarehouse(WarehouseID),
    MovementType NVARCHAR(50) CHECK (MovementType IN ('in', 'out', 'transfer')),
    Quantity INT NOT NULL,
    MovementDate INT REFERENCES DimDate(DateID),
    Reference NVARCHAR(255)
);

-- Ajout des Contraintes de Clés Étrangères pour DimProduct
ALTER TABLE DimProduct
ADD CONSTRAINT FK_DimProduct_DimProductCategory
FOREIGN KEY (CategoryID) REFERENCES DimProductCategory(CategoryID);