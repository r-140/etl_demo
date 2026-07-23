-- Create OLTP tables for each customer schema (3NF)
-- This script creates tables in all customer schemas

DO $$
DECLARE
    cust RECORD;
BEGIN
    FOR cust IN SELECT schema_name FROM public.customers_registry WHERE is_active = true
    LOOP
        -- Vendors table
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I.vendors (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255),
                phone VARCHAR(50),
                address TEXT,
                tax_id VARCHAR(50),
                status VARCHAR(20) DEFAULT ''active'',
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )', cust.schema_name);

        -- Categories table
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I.categories (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                parent_id INTEGER REFERENCES %I.categories(id),
                description TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )', cust.schema_name, cust.schema_name);

        -- Products table
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I.products (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                description TEXT,
                sku VARCHAR(100) UNIQUE,
                vendor_id INTEGER REFERENCES %I.vendors(id),
                category_id INTEGER REFERENCES %I.categories(id),
                price DECIMAL(10,2) NOT NULL,
                cost DECIMAL(10,2),
                stock_quantity INTEGER DEFAULT 0,
                status VARCHAR(20) DEFAULT ''active'',
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )', cust.schema_name, cust.schema_name, cust.schema_name);

        -- Customers table
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I.customers (
                id SERIAL PRIMARY KEY,
                first_name VARCHAR(100) NOT NULL,
                last_name VARCHAR(100) NOT NULL,
                email VARCHAR(255) UNIQUE,
                phone VARCHAR(50),
                segment VARCHAR(50) DEFAULT ''standard'',
                tier VARCHAR(20) DEFAULT ''bronze'',
                registration_date DATE DEFAULT CURRENT_DATE,
                last_activity_date DATE,
                status VARCHAR(20) DEFAULT ''active'',
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )', cust.schema_name);

        -- Orders table
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I.orders (
                id SERIAL PRIMARY KEY,
                customer_id INTEGER REFERENCES %I.customers(id),
                order_date DATE NOT NULL,
                order_timestamp TIMESTAMP DEFAULT NOW(),
                status VARCHAR(50) DEFAULT ''pending'',
                total_amount DECIMAL(12,2) DEFAULT 0,
                discount_amount DECIMAL(10,2) DEFAULT 0,
                shipping_address TEXT,
                payment_method VARCHAR(50),
                notes TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )', cust.schema_name, cust.schema_name);

        -- Order Items table
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I.order_items (
                id SERIAL PRIMARY KEY,
                order_id INTEGER REFERENCES %I.orders(id),
                product_id INTEGER REFERENCES %I.products(id),
                quantity INTEGER NOT NULL CHECK (quantity > 0),
                unit_price DECIMAL(10,2) NOT NULL,
                discount_percent DECIMAL(5,2) DEFAULT 0,
                line_total DECIMAL(12,2) GENERATED ALWAYS AS (
                    quantity * unit_price * (1 - discount_percent/100)
                ) STORED,
                created_at TIMESTAMP DEFAULT NOW()
            )', cust.schema_name, cust.schema_name, cust.schema_name);

        -- CDC tracking table (for WHALE customers)
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I.orders_cdc (
                cdc_id BIGSERIAL PRIMARY KEY,
                operation VARCHAR(10) NOT NULL,
                table_name VARCHAR(100),
                record_id INTEGER,
                old_data JSONB,
                new_data JSONB,
                changed_at TIMESTAMP DEFAULT NOW(),
                processed BOOLEAN DEFAULT false
            )', cust.schema_name);

        -- Create indexes
        EXECUTE format('
            CREATE INDEX IF NOT EXISTS idx_orders_customer ON %I.orders(customer_id)', cust.schema_name);
        EXECUTE format('
            CREATE INDEX IF NOT EXISTS idx_orders_date ON %I.orders(order_date)', cust.schema_name);
        EXECUTE format('
            CREATE INDEX IF NOT EXISTS idx_orders_updated ON %I.orders(updated_at)', cust.schema_name);
        EXECUTE format('
            CREATE INDEX IF NOT EXISTS idx_order_items_order ON %I.order_items(order_id)', cust.schema_name);
        EXECUTE format('
            CREATE INDEX IF NOT EXISTS idx_products_vendor ON %I.products(vendor_id)', cust.schema_name);
        EXECUTE format('
            CREATE INDEX IF NOT EXISTS idx_products_category ON %I.products(category_id)', cust.schema_name);

        -- Create trigger for updated_at
        EXECUTE format('
            CREATE OR REPLACE FUNCTION %I.update_updated_at()
            RETURNS TRIGGER AS $func$
            BEGIN
                NEW.updated_at = NOW();
                RETURN NEW;
            END;
            $func$ LANGUAGE plpgsql', cust.schema_name);

        -- Apply trigger to tables
        EXECUTE format('
            DROP TRIGGER IF EXISTS trg_customers_updated ON %I.customers;
            CREATE TRIGGER trg_customers_updated
                BEFORE UPDATE ON %I.customers
                FOR EACH ROW EXECUTE FUNCTION %I.update_updated_at()', 
                cust.schema_name, cust.schema_name, cust.schema_name);

        EXECUTE format('
            DROP TRIGGER IF EXISTS trg_orders_updated ON %I.orders;
            CREATE TRIGGER trg_orders_updated
                BEFORE UPDATE ON %I.orders
                FOR EACH ROW EXECUTE FUNCTION %I.update_updated_at()', 
                cust.schema_name, cust.schema_name, cust.schema_name);

        EXECUTE format('
            DROP TRIGGER IF EXISTS trg_products_updated ON %I.products;
            CREATE TRIGGER trg_products_updated
                BEFORE UPDATE ON %I.products
                FOR EACH ROW EXECUTE FUNCTION %I.update_updated_at()', 
                cust.schema_name, cust.schema_name, cust.schema_name);

        -- CDC trigger for orders (WHALE customers)
        IF cust.schema_name = 'customer_004' THEN
            EXECUTE format('
                CREATE OR REPLACE FUNCTION %I.orders_cdc_trigger()
                RETURNS TRIGGER AS $func$
                BEGIN
                    IF TG_OP = ''INSERT'' THEN
                        INSERT INTO %I.orders_cdc (operation, table_name, record_id, new_data)
                        VALUES (''INSERT'', ''orders'', NEW.id, to_jsonb(NEW));
                        RETURN NEW;
                    ELSIF TG_OP = ''UPDATE'' THEN
                        INSERT INTO %I.orders_cdc (operation, table_name, record_id, old_data, new_data)
                        VALUES (''UPDATE'', ''orders'', NEW.id, to_jsonb(OLD), to_jsonb(NEW));
                        RETURN NEW;
                    ELSIF TG_OP = ''DELETE'' THEN
                        INSERT INTO %I.orders_cdc (operation, table_name, record_id, old_data)
                        VALUES (''DELETE'', ''orders'', OLD.id, to_jsonb(OLD));
                        RETURN OLD;
                    END IF;
                    RETURN NULL;
                END;
                $func$ LANGUAGE plpgsql', 
                cust.schema_name, cust.schema_name, cust.schema_name, cust.schema_name);

            EXECUTE format('
                DROP TRIGGER IF EXISTS trg_orders_cdc ON %I.orders;
                CREATE TRIGGER trg_orders_cdc
                    AFTER INSERT OR UPDATE OR DELETE ON %I.orders
                    FOR EACH ROW EXECUTE FUNCTION %I.orders_cdc_trigger()', 
                    cust.schema_name, cust.schema_name, cust.schema_name);
        END IF;

    END LOOP;
END $$;
