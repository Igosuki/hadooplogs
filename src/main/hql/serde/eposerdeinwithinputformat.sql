ADD JAR /home/sesame/src/hivejars/eposerde.jar;
DROP TABLE IF EXISTS epo_normalized_table;
CREATE EXTERNAL TABLE IF NOT EXISTS epo_normalized_table (
	delivery_id string, provider string, flowtype string, publisher_doc_id string, batch_id string,
	status string, tstamp string, delivery string , year string, month string, XP string
)
ROW FORMAT SERDE 'load.serde.filters.CsvSerDe'
STORED AS INPUTFORMAT 'load.hadoop.mapred.CsvInputFormat' OUTPUTFORMAT 'load.hadoop.mapred.CsvOutputFormat'
LOCATION '/home/sesame/src/hivetests/epo/in'
TBLPROPERTIES (
'columns.types'='string,string,string,string,string,string,string,string,string,string,string',
'columns.names'='delivery_id, provider, flowtype, publisher_doc_id, batch_id, status, tstamp, delivery, year, month, XP',
'columns.addsep'='_',
'columns.default'=''
)
;
select * from epo_normalized_table limit 10;
