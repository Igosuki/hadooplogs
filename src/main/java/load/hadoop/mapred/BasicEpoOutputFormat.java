package load.hadoop.mapred;

import java.io.IOException;
import java.util.Properties;

import load.hadoop.writable.EpoWritable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;

public class BasicEpoOutputFormat extends FileOutputFormat<LongWritable, EpoWritable> implements HiveOutputFormat<LongWritable, EpoWritable> {

	@Override
	public RecordWriter getHiveRecordWriter(JobConf paramJobConf,
			Path paramPath, Class<? extends Writable> paramClass,
			boolean paramBoolean, Properties paramProperties,
			Progressable paramProgressable) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public org.apache.hadoop.mapred.RecordWriter<LongWritable, EpoWritable> getRecordWriter(
			FileSystem arg0, JobConf arg1, String arg2, Progressable arg3)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}
	

}
