DROP DATABASE IF EXISTS testdb;
CREATE DATABASE testdb;

CREATE TABLE test_table (
    id SERIAL PRIMARY KEY,
    last_updated DATE,
    value VARCHAR(50)
);

INSERT INTO test_table (last_updated, value)
VALUES 
('2000-01-01', 'row 1'),
('2010-01-01', 'row 2');