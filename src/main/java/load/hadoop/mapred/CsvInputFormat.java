package load.hadoop.mapred;

import java.io.IOException;
import java.io.InputStreamReader;

import load.hadoop.io.EpoBufferedReader;
import load.hadoop.writable.EpoWritable;
import load.model.EpoBaseEvent;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class CsvInputFormat extends
		FileInputFormat<LongWritable, Text> implements JobConfigurable {

	JobConf conf;

	@Override
	public void configure(JobConf conf) {
		this.conf = conf;
	}

	@Override
	protected boolean isSplitable(FileSystem fs, Path file) {
		return false;
	}

	@Override
	public RecordReader<LongWritable, Text> getRecordReader(
			InputSplit split, JobConf job, Reporter reporter)
			throws IOException {
		reporter.setStatus(split.toString());
		return new CsvRecordReader(job, (FileSplit) split);
	}
}
