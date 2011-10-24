-- This script creates the base table for the npl analisys
CREATE TABLE  npl_normalized_log (provider string, flowtype string, publisher_doc_id string, batch_id string, 
status string, tstamp string, delivery string , year string, month string, XP string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;'
STORED AS TEXTFILE;

CREATE TABLE npl_stat(provider string, flowtype string, delivery string, status string, status_count int, min_xp string, max_xp string);
