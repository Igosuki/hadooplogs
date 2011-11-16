ADD JAR /home/sesame/src/hivejars/eposerde.jar;
DROP TABLE IF EXISTS epo_normalized_table2;
CREATE EXTERNAL TABLE IF NOT EXISTS epo_normalized_table2 (field1 string, field2 string, field3 string)
ROW FORMAT SERDE 'load.serde.filters.CsvSerDe'  
LOCATION '/home/sesame/src/hivetests/epo/in'
TBLPROPERTIES (
'columns.types'='string,string,string',
'columns.names'='publisher_docid,batchid_in,time_stamp',
'columns.addsep'='_'
)
;
select * from epo_normalized_table2 limit 10;
INSERT OVERWRITE TABLE epo_normalized_table2 select publisher_docid, batchid_in, time_stamp from epo_normalized_table;
