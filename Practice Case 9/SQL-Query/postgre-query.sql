-- Create table in postgreSQL
CREATE TABLE bank_customers (
	customer_id int PRIMARY KEY,
	name varchar(255),
	gender varchar(50),
	age int,
	region varchar(50),
	job_classification varchar(255),
	balance float
);

-- Insert data to table bank-customers
INSERT INTO bank_customers (customer_id, name, gender, age, region, job_classification, balance)
VALUES (1, 'Simon', 'Male', 21, 'England', 'White Collar', 113810.15),
	 (2, 'Jasmine', 'Female', 34, 'Northern Ireland', 'Blue Collar', 36919.73),
	 (3, 'Liam', 'Male', 46, 'England', 'White Collar', 101536.83),
	 (4, 'Trevor', 'Male', 32, 'Wales', 'White Collar', 1421.52),
	 (5, 'Ava', 'Female', 30, 'Scotland', 'Blue Collar', 42879.84);