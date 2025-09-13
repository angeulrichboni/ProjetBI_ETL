-- Création des catégories de produits
CREATE TABLE product_categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL
);

-- Création des produits
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    category_id INT REFERENCES product_categories(id) ON DELETE SET NULL,
    price DECIMAL(10,2) NOT NULL,
    cost DECIMAL(10,2),
    uom VARCHAR(50),  -- Unité de mesure (Kg, unité, litre, etc.)
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Création des entrepôts
CREATE TABLE warehouses (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    location VARCHAR(255)
);

-- Gestion des stocks
CREATE TABLE stock (
    id SERIAL PRIMARY KEY,
    product_id INT REFERENCES products(id) ON DELETE CASCADE,
    warehouse_id INT REFERENCES warehouses(id),
    quantity INT NOT NULL DEFAULT 0,
    min_quantity INT DEFAULT 0,  -- Stock minimal pour alerte
    max_quantity INT DEFAULT 0,  -- Stock maximal
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Clients
CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE,
    phone VARCHAR(20),
    address TEXT
);

-- Fournisseurs
CREATE TABLE suppliers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    contact_email VARCHAR(255),
    contact_phone VARCHAR(20),
    address TEXT
);

-- Commandes Clients
CREATE TABLE sales_orders (
    id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(id),
    order_date TIMESTAMP DEFAULT NOW(),
    status VARCHAR(50) DEFAULT 'pending'  -- pending, confirmed, shipped, delivered
);

-- Détails des commandes clients
CREATE TABLE sales_order_lines (
    id SERIAL PRIMARY KEY,
    order_id INT REFERENCES sales_orders(id) ON DELETE CASCADE,
    product_id INT REFERENCES products(id),
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    total_price DECIMAL(10,2) GENERATED ALWAYS AS (quantity * unit_price) STORED
);

-- Commandes Fournisseurs
CREATE TABLE purchase_orders (
    id SERIAL PRIMARY KEY,
    supplier_id INT REFERENCES suppliers(id),
    order_date TIMESTAMP DEFAULT NOW(),
    status VARCHAR(50) DEFAULT 'pending'  -- pending, confirmed, received
);

-- Détails des commandes fournisseurs
CREATE TABLE purchase_order_lines (
    id SERIAL PRIMARY KEY,
    order_id INT REFERENCES purchase_orders(id) ON DELETE CASCADE,
    product_id INT REFERENCES products(id),
    quantity INT NOT NULL,
    unit_cost DECIMAL(10,2) NOT NULL,
    total_cost DECIMAL(10,2) GENERATED ALWAYS AS (quantity * unit_cost) STORED
);

-- Mouvements de stock (entrées et sorties)
CREATE TABLE stock_movements (
    id SERIAL PRIMARY KEY,
    product_id INT REFERENCES products(id),
    warehouse_id INT REFERENCES warehouses(id),
    movement_type VARCHAR(50) CHECK (movement_type IN ('in', 'out', 'transfer')),
    quantity INT NOT NULL,
    movement_date TIMESTAMP DEFAULT NOW(),
    reference VARCHAR(255)  -- Référence de commande client ou fournisseur
);
