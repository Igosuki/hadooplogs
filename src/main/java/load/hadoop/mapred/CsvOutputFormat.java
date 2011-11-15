package load.hadoop.mapred;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

public class CsvOutputFormat extends FileOutputFormat<LongWritable, Text> implements HiveOutputFormat<LongWritable, Text> {

	private static final Log LOG = LogFactory.getLog(CsvOutputFormat.class.getName());
	JobConf conf;

    @Override
    public RecordWriter<LongWritable, Text> getRecordWriter(FileSystem fs,
            JobConf job, String name, Progressable progress) throws IOException {
        Path outputPath = getWorkOutputPath(job);

        if (!fs.exists(outputPath)) {
            fs.mkdirs(outputPath);
        }
        Path file = new Path(outputPath, name);
        CompressionCodec codec = null;
        if (getCompressOutput(job)) {
            Class<?> codecClass = getOutputCompressorClass(job, DefaultCodec.class);
            codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, job);
        }
        
        final FSDataOutputStream outFile = fs.create(file);
        final BufferedOutputStream out = new BufferedOutputStream(outFile);
        StringBuilder sb = new StringBuilder();
		Iterator<Entry<String, String>> iterator = job.iterator();
		while(iterator.hasNext()) {
			Entry<String, String> next = iterator.next();
			sb.append(" " + next.getKey() + " : " + next.getValue() + ", ");
		}
        LOG.info("Doing file : " + name + " , with Job properties : " + sb.toString());
        return new RecordWriter<LongWritable, Text>() {

            @Override
            public void close(Reporter reporter) throws IOException {
                out.close();
            }

            @Override
            public void write(LongWritable key, Text value)
                    throws IOException {
                out.write(value.getBytes());
            }
        };
    }
    

    @Override
    public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf job,
            Path path, Class type, boolean isCompressed, Properties properties,
            Progressable p) throws IOException {
        FileSystem fs = path.getFileSystem(job);

        final BufferedOutputStream outStream = new  BufferedOutputStream(Utilities.createCompressedStream(job,
                fs.create(path), isCompressed));

        int rowSeparator = 0;
        String rowSeparatorString = properties.getProperty(
                Constants.LINE_DELIM, "\n");
        try {
            rowSeparator = Byte.parseByte(rowSeparatorString);
        } catch (NumberFormatException e) {
            rowSeparator = rowSeparatorString.charAt(0);
        }
        final int finalRowSeparator = rowSeparator;
        return new FileSinkOperator.RecordWriter() {

            @Override
            public void write(Writable r) throws IOException {
                if (r instanceof Text) {
                    Text tr = (Text) r;
                    LOG.info("Writing text :"+ tr.toString());
                    outStream.write(tr.getBytes(), 0, tr.getLength());
                    outStream.write(finalRowSeparator);
                } else {
                    BytesWritable bw = (BytesWritable) r;
                    outStream.write(bw.getBytes(), 0, bw.getLength());
                    outStream.write('\n');
                }
            }

            @Override
            public void close(boolean abort) throws IOException {
                outStream.close();
            }
        };

    }
}
