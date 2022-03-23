create database monitor_result;
\c monitor_result;

CREATE TABLE result_info(
id serial PRIMARY KEY NOT NULL,
access_key varchar(200) NOT NULL,
stream_url varchar(200) NOT NULL,
stream_id varchar(200) NOT NULL,
result TEXT NOT NULL,
timestamp timestamp NOT NULL,
catchDate date NOT NULL
);
