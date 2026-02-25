-- Test database initialization script
-- This runs automatically when the PostgreSQL container starts

-- Create test tables
CREATE TABLE IF NOT EXISTS employees (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    department TEXT,
    salary NUMERIC(10, 2),
    active BOOLEAN NOT NULL DEFAULT true,
    hired_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS departments (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    budget NUMERIC(12, 2)
);

-- Seed data
INSERT INTO departments (name, budget) VALUES
    ('Engineering', 500000.00),
    ('Marketing', 200000.00),
    ('Sales', 300000.00);

INSERT INTO employees (name, email, department, salary, active) VALUES
    ('Alice', 'alice@example.com', 'Engineering', 95000.00, true),
    ('Bob', 'bob@example.com', 'Engineering', 90000.00, true),
    ('Charlie', 'charlie@example.com', 'Marketing', 75000.00, true),
    ('Diana', 'diana@example.com', 'Sales', 80000.00, false),
    ('Eve', 'eve@example.com', 'Engineering', 105000.00, true);
