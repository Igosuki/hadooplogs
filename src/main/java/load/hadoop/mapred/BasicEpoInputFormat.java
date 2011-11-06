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
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class BasicEpoInputFormat extends
		FileInputFormat<LongWritable, EpoWritable> implements JobConfigurable {

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
	public RecordReader<LongWritable, EpoWritable> getRecordReader(
			InputSplit split, JobConf job, Reporter reporter)
			throws IOException {
		reporter.setStatus(split.toString());
		return new EpoRecordReader(job, (FileSplit) split);
	}

	/*************************************************************
	 * RecordReader
	 * 
	 *************************************************************/
	public static class EpoRecordReader implements
			RecordReader<LongWritable, EpoWritable> {

		private static final Log LOG = LogFactory.getLog(EpoRecordReader.class
				.getName());
		private CompressionCodecFactory compressionCodecs = null;
		private long start;
		private long pos;
		private long end;
		private final EpoBufferedReader in;

		public EpoRecordReader(Configuration job, FileSplit split)
				throws IOException {
			//int maxlines = job.getInt("mapred.maxlines", Integer.MAX_VALUE);
			start = split.getStart();
			end = start + split.getLength();
			final Path file = split.getPath();

			LOG.debug("RecordReader: processing path " + file.toString());

			compressionCodecs = new CompressionCodecFactory(job);
			final CompressionCodec codec = compressionCodecs.getCodec(file);

			// open the file and seek to the start of the split
			FileSystem fs = file.getFileSystem(job);
			FSDataInputStream fileIn = fs.open(split.getPath());

			if (codec != null) {
				in = new EpoBufferedReader(split.getPath().getName(), new InputStreamReader(codec.createInputStream(fileIn)));
			} else {
				in = new EpoBufferedReader(split.getPath().getName(), new InputStreamReader(fileIn));
			}
		}

		@Override
		public LongWritable createKey() {
			return new LongWritable();
		}

		@Override
		public EpoWritable createValue() {
			return new EpoWritable();
		}

		/** Read a line. */
		@Override
		public synchronized boolean next(LongWritable key, EpoWritable value)
				throws IOException {
			EpoBaseEvent ev = in.readEvent();

			if (ev != null) {
				value.setEvent(ev);
				
				pos += ev.serialize().length;

				key.set(pos);

				return true;
			} else {
				return false;
			}
		}

		@Override
		public float getProgress() {
			if (start == end) {
				return 0.0f;
			} else {
				return Math.min(1.0f, (pos - start) / (float) (end - start));
			}
		}

		@Override
		public synchronized long getPos() throws IOException {
			return pos;
		}

		@Override
		public synchronized void close() throws IOException {
			if (in != null) {
				in.close();
			}
		}
	}

}
