-- This script creates the base table for the npl analisys
CREATE TABLE  npl_normalized_log (delivery_id string, provider string, flowtype string, publisher_doc_id string, batch_id string,
status string, tstamp string, delivery string , year string, month string, XP string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;'
STORED AS TEXTFILE;

CREATE TABLE npl_stat(delivery_id string, provider string, flowtype string, delivery string, status string, status_count int, min_xp string, max_xp string);

CREATE TABLE npl_files_out(delivery_id string, txt_file string, bbl_file string);

CREATE TABLE npl_stat_condensed(DELIVERY_ID string, PROVIDER string, FLOWTYPE string, DELIVERY string, INN string, IGNORED string,
BUSINNESSREJECT string, UNKNOWNERROR string, NOPDF string, UNKNOWNCONTENTTYPE string,OK string, PRODUCED string, FIRST_XP string, LAST_XP string,
BBL_FILE string, TXT_FILE string, ELABORATION_DATE string, RUN_DATE string);
