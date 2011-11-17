ADD JAR /home/sesame/src/hivejars/eposerde.jar;
DROP TABLE IF EXISTS epont;
CREATE EXTERNAL TABLE IF NOT EXISTS epont (field1 string, field2 string, field3 string)
ROW FORMAT SERDE 'load.serde.filters.CsvSerDe'  
LOCATION '/var/hadoop/data/epoin'
TBLPROPERTIES (
'columns.types'='string,string,string',
'columns.names'='publisher_docid,batchid_in,time_stamp',
'columns.transform.names'='ieee,, publisher_docid,batchid_in,time_stamp',
'columns.addsep'='_'
'columns.default'=''
)
;
select * from epont limit 10;
