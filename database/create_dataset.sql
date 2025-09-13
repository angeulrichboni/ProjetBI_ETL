CREATE TABLE ERPDataset (
    DatasetID INT IDENTITY(1,1) PRIMARY KEY, -- Identifiant unique du dataset
    CustomerID INT NOT NULL,                 -- Identifiant du client
    CustomerName NVARCHAR(255) NOT NULL,    -- Nom du client
    Email NVARCHAR(255),                    -- Email du client
    Phone NVARCHAR(20),                     -- Téléphone du client
    Address NVARCHAR(MAX),                  -- Adresse du client
    ProductID INT NOT NULL,                 -- Identifiant du produit
    ProductName NVARCHAR(255) NOT NULL,     -- Nom du produit
	CategoryName NVARCHAR(255) NOT NULL,     -- Nom du produit
    CategoryID INT NOT NULL,                -- Catégorie du produit
    Price DECIMAL(10,2) NOT NULL,           -- Prix du produit
    Cost DECIMAL(10,2) NOT NULL,            -- Coût du produit
    OrderDate DATE NOT NULL,                -- Date de la commande
    Day INT NOT NULL,                       -- Jour de la commande
    Month INT NOT NULL,                     -- Mois de la commande
    Year INT NOT NULL,                      -- Année de la commande
    Quarter INT NOT NULL,                   -- Trimestre de la commande
    Quantity INT NOT NULL,                  -- Quantité commandée
    TotalPrice DECIMAL(10,2) NOT NULL,      -- Montant total de la commande
    AvgOrderValue DECIMAL(10,2),            -- Valeur moyenne des commandes du client
    FavoriteCategory NVARCHAR(255),         -- Catégorie de produit préférée du client
    TotalOrders INT,                        -- Nombre total de commandes du client
    TotalQuantity INT,                      -- Quantité totale commandée par le client
    LastOrderDate DATE,                     -- Date de la dernière commande du client
    DaysSinceLastOrder INT                  -- Jours écoulés depuis la dernière commande
);



WITH SalesAggregates AS (
    SELECT 
        fs.CustomerID,
        COUNT(*) AS TotalOrders,
        SUM(fs.Quantity) AS TotalQuantity,
        MAX(dd.Date) AS LastOrderDate,
        AVG(fs.TotalPrice) AS AvgOrderValue
    FROM FactSales fs
    JOIN DimDate dd ON fs.OrderDate = dd.DateID
    GROUP BY fs.CustomerID
),
FavoriteCategory AS (
    SELECT 
        fs.CustomerID,
        dp3.CategoryName,
        RANK() OVER (PARTITION BY fs.CustomerID ORDER BY COUNT(*) DESC) AS CategoryRank
    FROM FactSales fs
    JOIN DimProduct dp2 ON fs.ProductID = dp2.ProductID
    JOIN DimProductCategory dp3 ON dp2.CategoryID = dp3.CategoryID
    GROUP BY fs.CustomerID, dp3.CategoryName
)
SELECT TOP (10)
    fs.CustomerID,
    dc.CustomerName,
    dc.Email,
    dc.Phone,
    dc.Address,
    fs.ProductID,
    dp.ProductName,
    dpc.CategoryName,
    dp.CategoryID,
    dp.Price,
    dp.Cost,
    dd.Date AS OrderDate,
    dd.Day,
    dd.Month,
    dd.Year,
    dd.Quarter,
    fs.Quantity,
    fs.TotalPrice,
    sa.AvgOrderValue,
    fc.CategoryName AS FavoriteCategory,
    sa.TotalOrders,
    sa.TotalQuantity,
    sa.LastOrderDate,
    DATEDIFF(DAY, sa.LastOrderDate, GETDATE()) AS DaysSinceLastOrder
FROM FactSales fs
JOIN DimCustomer dc ON fs.CustomerID = dc.CustomerID
JOIN DimProduct dp ON fs.ProductID = dp.ProductID
JOIN DimDate dd ON fs.OrderDate = dd.DateID
JOIN DimProductCategory dpc ON dp.CategoryID = dpc.CategoryID
JOIN SalesAggregates sa ON fs.CustomerID = sa.CustomerID
LEFT JOIN FavoriteCategory fc ON fs.CustomerID = fc.CustomerID AND fc.CategoryRank = 1
ORDER BY NEWID()
OPTION (MAXDOP 8); -- Permet de paralléliser jusqu'à 8 threads