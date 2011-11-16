package load.hadoop.mapred;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

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

	
	private static final String FLOWTYPE_NAME = "flowType";

	private static final String PROVIDER_NAME = "provider";
	
	private static final String INPUTTYPE_NAME = "name";

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
	private Map<String, String> manualDefaults = new HashMap<String, String>();
	
	public CsvRecordReader(Configuration job, FileSplit split)
			throws IOException {
		this.start = split.getStart();
		this.end = this.start + split.getLength();
		final Path file = split.getPath();
		
		//Manual 
		LOG.info("Reading filesplit with path " + file.toString() + " and name : " +  file.getName());
		String[] fileTokens = file.getName().split("_");
		LOG.info("File tokens : " + StringUtils.join(fileTokens, "," ));
		String[] constTokens = fileTokens[2].split("\\.");
		LOG.info("Const tokens : " + StringUtils.join(constTokens, "," ));
		manualDefaults.put(PROVIDER_NAME, constTokens[0]);
		manualDefaults.put(FLOWTYPE_NAME, constTokens[1]);
		manualDefaults.put(INPUTTYPE_NAME, fileTokens[3].split("\\.")[0]);
		
		//End manual
		
		LOG.debug("RecordReader: processing path " + file.toString());
		String formatProperties = job.get("hadoop.csvinput.format");
		String expectedCols = null;
		String actualCols = null;
		//Read 
		if (!StringUtils.isBlank(formatProperties)) {
			//Try to get them from a properties file in the classpath
			Properties inProp = new Properties();
					new BufferedReader(new InputStreamReader(getClass()
					.getClassLoader().getResourceAsStream(
							formatProperties + ".properties")));
			LOG.info("Reading properties file from classpath : " + formatProperties);
			expectedCols = inProp.getProperty("columns.expected");
			actualCols = inProp.getProperty("columns.mapping");
		} else {
			//Try to get them from the job conf
			expectedCols = job.get("columns.expected");
			actualCols = job.get("columns.mapping");
		}
		isNormalizing = getNormalizing(expectedCols, actualCols);
		
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
		
		LOG.info("Doing file : " + split.getPath().toString()+ " , with Job properties : " + sb.toString());
		this.in = new LineReader(fileIn, job);
	}

	private boolean getNormalizing(String expectedCols, String actualCols) {
		if(!StringUtils.isBlank(actualCols) && !StringUtils.isBlank(expectedCols)) {
			String[] acSplit = actualCols.split(propSplit);
			String[] exSplit = expectedCols.split(propSplit);
			columnsMapping = new HashMap<Integer, Integer>();
			columnDefaults = new ArrayList<String>();
			for (int i = 0; i < acSplit.length; i++) {
				this.columnsMapping.put(Integer.valueOf(acSplit[i]), i);
			}
			for (int i = 0; i < exSplit.length; i++) {
				this.columnDefaults.add(exSplit[i]);
			}
			return true;
		} 
		return false;
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
		pos += this.in.readLine(value);
		value.toString();
		if(isNormalizing) {
			final String[] actual = value.toString().split(csvSplit);
			final String[] newLine = new String[columnDefaults.size()];
			for (int i = 0; i < newLine.length; i++) {
				if(columnsMapping.get(i) != null) {
					final String string = actual[columnsMapping.get(i)];
					if(StringUtils.isBlank(string)) {
						newLine[i] = nullValue;
					} else {
						newLine[i] = string;
					}
				} else {
					newLine[i] = columnDefaults.get(i);
				}
			}
		}
		// Do it manually for now
		StringBuilder rebuild = new StringBuilder();
		
		try {
			final String[] actualSplit = value.toString().split(";");
			String deliveryInfo = actualSplit[0].split("_")[0];
			String timestamp = actualSplit[2].toUpperCase();
			rebuild.append(manualDefaults.get(PROVIDER_NAME)).append("_");
			rebuild.append(manualDefaults.get(FLOWTYPE_NAME)).append("_");
			rebuild.append(deliveryInfo).append(";");
			rebuild.append(manualDefaults.get(PROVIDER_NAME)).append(";");
			rebuild.append(manualDefaults.get(FLOWTYPE_NAME)).append(";");
			rebuild.append(actualSplit[0].toUpperCase()).append(";");
			rebuild.append(actualSplit[1].toUpperCase()).append(";");
			rebuild.append(manualDefaults.get(INPUTTYPE_NAME)).append(";");
			rebuild.append(timestamp).append(";");
			rebuild.append(deliveryInfo).append(";");
			rebuild.append(timestamp.substring(0, 3)).append(";");
			rebuild.append(timestamp.substring(4, 5)).append(";");
			rebuild.append("null").append(";");
			value.set(rebuild.toString());
		} catch (Exception e) {
			LOG.info(e.getMessage());
			return false;
		}
		LOG.info(value.toString());
		key.set(pos);
		if (value.getLength() > 0) {
			return true;
		} else {
			return false;
		}
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