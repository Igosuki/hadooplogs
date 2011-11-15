ADD JAR /home/sesame/src/hivejars/eposerde.jar;
DROP TABLE IF EXISTS epo_normalized_table;
CREATE EXTERNAL TABLE IF NOT EXISTS epo_normalized_table (field1 string, field2 string, field3 string)
ROW FORMAT SERDE 'load.serde.filters.CsvSerDe'  
LOCATION '/home/sesame/src/hivetests/epo/in'
TBLPROPERTIES (
'columns.types'='string,string,string',
'columns.names'='publisher_docid,batchid_in,time_stamp',
'columns.transform.names'='ieee,, publisher_docid,batchid_in,time_stamp',
'columns.addsep'='_'
'columns.default'=''
)
;
select * from epo_normalized_table limit 10;
