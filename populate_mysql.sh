#!/bin/bash
set -e

# Wait for MySQL to be ready
echo "Waiting for MySQL to be ready..."
until sudo docker exec mysql-db mysql -uapp_user -pAppPass123! -e "SELECT 1" &> /dev/null; do
  echo "MySQL is unavailable - sleeping"
  sleep 2
done

echo "MySQL is up - creating table and inserting data..."

sudo docker exec -i mysql-db mysql -uapp_user -pAppPass123! sample_db <<EOF
CREATE TABLE IF NOT EXISTS employees (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100),
    position VARCHAR(100),
    salary INT,
    hire_date DATE
);

INSERT INTO employees (name, position, salary, hire_date) VALUES 
('Alice Smith', 'Engineer', 90000, '2023-01-15'),
('Bob Jones', 'Manager', 120000, '2022-05-20'),
('Charlie Brown', 'Analyst', 75000, '2023-08-10'),
('Diana Prince', 'Director', 150000, '2021-11-01'),
('Evan Wright', 'Developer', 85000, '2023-03-30');

-- Generate more data (simple duplication for volume)
INSERT INTO employees (name, position, salary, hire_date) 
SELECT name, position, salary, hire_date FROM employees;
INSERT INTO employees (name, position, salary, hire_date) 
SELECT name, position, salary, hire_date FROM employees;
INSERT INTO employees (name, position, salary, hire_date) 
SELECT name, position, salary, hire_date FROM employees;
INSERT INTO employees (name, position, salary, hire_date) 
SELECT name, position, salary, hire_date FROM employees;

EOF

echo "Data population complete."
