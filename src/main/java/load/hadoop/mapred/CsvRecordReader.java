package load.hadoop.mapred;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import load.hadoop.conf.ConfigurationContext;

import org.apache.commons.lang.StringUtils;
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
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.LineReader;

/*************************************************************
 * RecordReader
 * 
 *************************************************************/
public class CsvRecordReader implements RecordReader<LongWritable, Text> {

	
	private static final Log LOG = LogFactory.getLog(CsvRecordReader.class.getName());
	
	private static final String csvSplit = ";";
	private static final String propSplit = ",";
	private static final String nullValue = "null";
    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    int maxLineLength;
    private final LineReader in;
    
    /**
     * Configuration context for normalization
     */
    private ConfigurationContext ctxt;
    
    /**
	 * Column Names in the table
	 */
	private List<String> columnDefaults;
	/**
	 * Column Names in the table
	 */
	private Map<Integer, Integer> columnsMapping;
	/**
	 * If the reader is normalizing
	 */
	private Boolean isNormalizing = false;
	/**
	 * Column Names in the table
	 */
	private Map<String, String> manualFileNameConf = new LinkedHashMap<String, String>();
	
	public CsvRecordReader(Configuration job, FileSplit split)
			throws IOException {
		this.start = split.getStart();
		this.pos = this.start;
		this.end = this.start + split.getLength();
		final Path file = split.getPath();
		
		LOG.debug("RecordReader: processing path " + file.toString());
		
		manualFileNameConf.put("interm_in.csv", "interm");
		manualFileNameConf.put("_in.csv", "in");
		manualFileNameConf.put("_loader.csv", "loader");
		String formatProperties = null;
		for (Entry<String, String> entry : manualFileNameConf.entrySet()) {
			if (file.getName().endsWith(entry.getKey())) {
				formatProperties = entry.getValue()+".xml";
				break;
			}
		}
	    if(formatProperties == null) {
	    	job.get("hadoop.csvinput.format");
	    }

		//Read 
		if ((isNormalizing = !StringUtils.isBlank(formatProperties))) {
			//Try to get them from a properties file in the classpath
			this.ctxt = new ConfigurationContext(split.getPath().getName(), 
				getClass().getClassLoader().getResourceAsStream(formatProperties)
			);
			LOG.info("Reading properties file from classpath : " + formatProperties);
		} 
		
		this.compressionCodecs = new CompressionCodecFactory(job);
		final CompressionCodec codec = this.compressionCodecs.getCodec(file);

		// open the file and seek to the start of the split
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath());
		
		StringBuilder sb = new StringBuilder();
		Iterator<Entry<String, String>> iterator = job.iterator();
		while(iterator.hasNext()) {
			Entry<String, String> next = iterator.next();
			sb.append(" " + next.getKey() + " : " + next.getValue() + ", ");
		}
		
		LOG.debug("Doing file : " + split.getPath().toString()+ " , with Job properties : " + sb.toString());
		this.in = new LineReader(fileIn, job);
		
		
	}

	@Override
	public LongWritable createKey() {
		return new LongWritable();
	}

	@Override
	public Text createValue() {
		return new Text();
	}

	/** Read a line. */
	@Override
	public synchronized boolean next(LongWritable key, Text value)
			throws IOException {
		key.set(this.pos);
		int newsize = this.in.readLine(value);
		if(newsize == 0) {
			return false;
		}
		this.pos += newsize;
		try {
			if(isNormalizing) {
				ctxt.parseContent(value.toString());
				value.set(ctxt.flush());
				LOG.debug(value.toString());
			}
		} catch(Exception e ) {
			LOG.debug(e.getMessage());
			return false;
		}
		
		if (newsize > 0) {
			return true;
		} 
		return false;
	}

	@Override
	public float getProgress() {
		if (this.start == this.end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (this.pos - this.start) / (float) (this.end - this.start));
		}
	}

	@Override
	public synchronized long getPos() throws IOException {
		return this.pos;
	}

	@Override
	public synchronized void close() throws IOException {
		if (this.in != null) {
			this.in.close();
		}
	}
}