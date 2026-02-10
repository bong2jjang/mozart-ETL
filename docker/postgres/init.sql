-- Sample tenant database initialization
-- Creates sample tables for development/testing

CREATE TABLE IF NOT EXISTS public.customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES public.customers(id),
    amount DECIMAL(10, 2),
    status VARCHAR(50) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data
INSERT INTO public.customers (name, email) VALUES
    ('Alice Kim', 'alice@example.com'),
    ('Bob Park', 'bob@example.com'),
    ('Charlie Lee', 'charlie@example.com'),
    ('Diana Choi', 'diana@example.com'),
    ('Eve Jung', 'eve@example.com');

INSERT INTO public.orders (customer_id, amount, status) VALUES
    (1, 150.00, 'completed'),
    (1, 75.50, 'completed'),
    (2, 200.00, 'pending'),
    (3, 50.00, 'completed'),
    (3, 125.00, 'shipped'),
    (4, 300.00, 'completed'),
    (5, 89.99, 'pending');
