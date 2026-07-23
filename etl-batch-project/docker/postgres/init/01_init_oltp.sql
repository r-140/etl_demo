-- Initialize OLTP database with multi-tenant schema support
-- Each customer gets their own schema

-- Create extension for UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create customers registry table
CREATE TABLE IF NOT EXISTS public.customers_registry (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL,
    customer_size VARCHAR(20) NOT NULL CHECK (customer_size IN ('SMALL', 'MEDIUM', 'LARGE', 'WHALE')),
    storage_type VARCHAR(10) NOT NULL CHECK (storage_type IN ('OLTP', 'OLAP')),
    delta_strategy VARCHAR(50),
    schema_name VARCHAR(100) NOT NULL,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Create sample customers
INSERT INTO public.customers_registry (customer_id, customer_name, customer_size, storage_type, schema_name)
VALUES 
    ('cust_001', 'Acme Small Corp', 'SMALL', 'OLTP', 'customer_001'),
    ('cust_002', 'Beta Medium Inc', 'MEDIUM', 'OLTP', 'customer_002'),
    ('cust_003', 'Gamma Large LLC', 'LARGE', 'OLAP', 'customer_003'),
    ('cust_004', 'Delta Whale Enterprise', 'WHALE', 'OLAP', 'customer_004')
ON CONFLICT (customer_id) DO NOTHING;

-- Create schemas for each customer
DO $$
DECLARE
    cust RECORD;
BEGIN
    FOR cust IN SELECT schema_name FROM public.customers_registry WHERE is_active = true
    LOOP
        EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', cust.schema_name);
    END LOOP;
END $$;
