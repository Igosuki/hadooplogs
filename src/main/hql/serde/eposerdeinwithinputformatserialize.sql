ADD JAR /home/sesame/src/hivejars/eposerde.jar;
DROP TABLE IF EXISTS epont;
CREATE EXTERNAL TABLE IF NOT EXISTS epont (
	delivery_id string, provider string, flowtype string, publisher_doc_id string, batch_id string,
	status string, tstamp string, delivery string , year string, month string, XP string
)
COMMENT 'This is a table for logs of DFP processes'
ROW FORMAT SERDE 'load.serde.filters.CsvSerDe'
STORED AS INPUTFORMAT 'load.hadoop.mapred.CsvInputFormat' OUTPUTFORMAT 'load.hadoop.mapred.CsvOutputFormat'
LOCATION '/var/hadoop/data/epoin'
TBLPROPERTIES (
'columns.types'='string,string,string,string,string,string,string,string,string,string,string',
'columns.names'='delivery_id,provider,flowtype,publisher_doc_id,batch_id,status,tstamp,delivery,year,month,XP'
)
;
select * from epont limit 10;
INSERT OVERWRITE TABLE epont2 select publisher_docid, batchid_in, time_stamp from epont;
