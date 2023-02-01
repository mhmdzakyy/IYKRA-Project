-- Create source connector to postgre
CREATE SOURCE CONNECTOR jdbc_source WITH (
  'connector.class'          = 'io.confluent.connect.jdbc.JdbcSourceConnector',
  'connection.url'           = 'jdbc:postgresql://postgres:5432/postgres',
  'connection.user'          = 'postgres',
  'connection.password'      = 'password',
  'topic.prefix'             = 'jdbc_',
  'table.whitelist'          = 'bank_customers',
  'mode'                     = 'incrementing',
  'numeric.mapping'          = 'best_fit',
  'incrementing.column.name' = 'customer_id',
  'key'                      = 'customer_id',
  'key.converter'            = 'org.apache.kafka.connect.converters.IntegerConverter');

  -- Create Streams
CREATE STREAM stream_table(
    customer_id INT,
    name VARCHAR,
    gender VARCHAR,
    age INT,
    region VARHCAR,
    job_classification VARCHAR, 
    balance float
) WITH (kafka_topics='jdbc_bank_customers', value_format='json', partitions=1);

-- Create materialized table
CREATE TABLE final_table as
    SELECT st.name, count(*) as transaction_times, sum(balance) as total_balance 
    FROM stream_table st
    GROUP BY st.name
    emit changes;

-- insert data to stream table
INSERT INTO stream_table (customer_id, name, gender, age, region, job_classification, balance) VALUES (1, 'Simon', 'Male', 21, 'England', 'White Collar', 113810.15);
INSERT INTO stream_table (customer_id, name, gender, age, region, job_classification, balance) VALUES (2, 'Jasmine', 'Female', 34, 'Northern Ireland', 'Blue Collar', 36919.73);
INSERT INTO stream_table (customer_id, name, gender, age, region, job_classification, balance) VALUES (3, 'Liam', 'Male', 46, 'England', 'White Collar', 101536.83);
INSERT INTO stream_table (customer_id, name, gender, age, region, job_classification, balance) VALUES (4, 'Trevor', 'Male', 32, 'Wales', 'White Collar', 1421.52);
INSERT INTO stream_table (customer_id, name, gender, age, region, job_classification, balance) VALUES (5, 'Ava', 'Female', 30, 'Scotland', 'Blue Collar', 42879.84);
INSERT INTO stream_table (customer_id, name, gender, age, region, job_classification, balance) VALUES (1, 'Simon', 'Male', 21, 'England', 'White Collar', 10.15);

-- get final_table data 
select * from final_table WHERE total_balance >= 30000;