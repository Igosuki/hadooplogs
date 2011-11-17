ADD JAR /home/sesame/src/hivejars/eposerde.jar;
DROP TABLE IF EXISTS epont2;
CREATE EXTERNAL TABLE IF NOT EXISTS epont2 (field1 string, field2 string, field3 string)
ROW FORMAT SERDE 'load.serde.filters.CsvSerDe'  
LOCATION '/var/hadoop/data/epoin'
TBLPROPERTIES (
'columns.types'='string,string,string',
'columns.names'='publisher_docid,batchid_in,time_stamp',
'columns.addsep'='_'
)
;
select * from epont2 limit 10;
INSERT OVERWRITE TABLE epont2 select publisher_docid, batchid_in, time_stamp from epont;
