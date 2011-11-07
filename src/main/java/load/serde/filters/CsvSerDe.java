package load.serde.filters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import load.model.EventException;
import load.utils.SerDeStringUtils;
import load.utils.Constants.TableProps;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class CsvSerDe implements SerDe {

	/**
	 * Apache commons logger
	 */
	private static final Log LOG = LogFactory.getLog(CsvSerDe.class.getName());
	
	/**
	 * The number of columns in the table
	 */
	private int nbColumns;
	
	/**
	 * Column Names in the table
	 */
	private List<String> columnNames;
	
	/**
	 * Column Types in the table
	 */
	private List<TypeInfo> columnTypes;
	
	/**
	 * An object inspector that wraps a deserialized row
	 */
	private StructObjectInspector rowOI;
	
	/**
	 * ArrayList of row objects 
	 */
	private ArrayList<Object> row;
	
	/**
	 * Additional separator char 
	 */
	private String addSepChar = "_";
	
	/**
	 * Initialize the SerDe with properties
	 */
	@Override
	public void initialize(Configuration conf, Properties tprops)
			throws SerDeException {
		String colNames = tprops.getProperty(TableProps.COLUMNS);
		columnNames = Arrays.asList(colNames.split(","));
		
		String colTypes = tprops.getProperty(TableProps.COLUMN_TYPES);
		columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(colTypes);
		
		String addChar = tprops.getProperty(TableProps.ADD_SEP_CHAR);
		addSepChar = StringUtils.isBlank(addChar) ? addSepChar : addChar; 
		
		assert columnNames.size() == columnTypes.size();
		nbColumns = columnNames.size();
		
		// Create Object Inspectors for each col
		List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(nbColumns);
		ObjectInspector oi;
		for (int i = 0; i < nbColumns; i++) {
			oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(columnTypes.get(i));
			columnOIs.add(oi);
		}
		rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs);
		
		// Create a row of objects that will hold items during deserialization
		row = new ArrayList<Object>(nbColumns);
		for (int j = 0; j < nbColumns; j++) {
			// Fix pointers
			row.add(null);
		}
		LOG.debug("Successfully initialized CsvSerde");
	}
	
	
	@Override
	public Object deserialize(Writable arg0) throws SerDeException {
		Text rowText = (Text) arg0;
		LOG.debug("Deserializing row : " + rowText.toString());

		String[] cols = rowText.toString().split(";");

		// Simple iteration through columns
		String colName;
		Object value;
		for (int c = 0; c < nbColumns; c++) {
			colName = columnNames.get(c);
			TypeInfo ti = columnTypes.get(c);

			try {
				// Get type-safe JSON values
				if (SerDeStringUtils.isNull(cols[c])) {
					value = null;
				} else if (ti.getTypeName().equalsIgnoreCase(
						Constants.DOUBLE_TYPE_NAME)) {
					value = SerDeStringUtils.getDouble(cols[c]);
				} else if (ti.getTypeName().equalsIgnoreCase(
						Constants.BIGINT_TYPE_NAME)) {
					value = SerDeStringUtils.getLong(cols[c]);
				} else if (ti.getTypeName().equalsIgnoreCase(
						Constants.INT_TYPE_NAME)) {
					value = SerDeStringUtils.getInt(cols[c]);
				} else if (ti.getTypeName().equalsIgnoreCase(
						Constants.TINYINT_TYPE_NAME)) {
					value = Byte.valueOf(SerDeStringUtils.getString(cols[c]));
				} else if (ti.getTypeName().equalsIgnoreCase(
						Constants.FLOAT_TYPE_NAME)) {
					value = Float.valueOf(SerDeStringUtils.getString(cols[c]));
				} else if (ti.getTypeName().equalsIgnoreCase(
						Constants.BOOLEAN_TYPE_NAME)) {
					value = SerDeStringUtils.getBoolean(cols[c]);
				} else if (ti.getTypeName().equalsIgnoreCase(
						Constants.STRING_TYPE_NAME)) {
					value = SerDeStringUtils.getString(cols[c]);
				} else if (ti.getTypeName()
						.startsWith(Constants.LIST_TYPE_NAME)) {
					// Parse this 
					String[] newarr = cols[c].split(addSepChar);
					value = newarr;

				} else {
					// Unknown type but there is a value
					value = cols[c];
				}
			} catch (EventException e) {
				// Exception, set to null and skip
				if (LOG.isDebugEnabled()) {
					LOG.debug("Column '" + colName + "' not found in row: "
							+ rowText.toString() + " - EventException: "
							+ e.getMessage());
				}
				value = null;
			}
			row.set(c, value);
		}
		return row;
	}

	@Override
	public Writable serialize(Object arg0, ObjectInspector arg1)
			throws SerDeException {
		// TODO Auto-generated method stub
		return null;
	}
	
	/**
	 * The Writable class the object is serialized into
	 */
	@Override
	public Class<? extends Writable> getSerializedClass() {
		return Text.class;
	}
	
	/**
	 * Gets the object inspector for a row parsed by Hive
	 */
	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return rowOI;
	}
}
