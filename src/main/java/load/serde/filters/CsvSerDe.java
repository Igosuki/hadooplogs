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
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
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
	 * Column Names in the table
	 */
	private List<Object> columnDefaults;
	
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

//		String colDefaults = tprops.getProperty(TableProps.COLUMN_DEFAULTS);
//		if(!StringUtils.isBlank(colDefaults)) {
//			List<String> colDefaultsProp = Arrays.asList(colDefaults.split(",")); 
//			if(colDefaultsProp.size() == columnNames.size()) {
//				columnDefaults = new ArrayList<Object>(nbColumns);
//				for (int i = 0; i < nbColumns; i++) {
//					columnDefaults.add(deserializeField(colDefaultsProp.get(i), columnTypes.get(i)));
//				}
//			}
//		}
		
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

		String[] fields = rowText.toString().split(";");

		// Simple iteration through columns
		String colName;
		Object value;
		for (int c = 0; c < nbColumns; c++) {
			colName = columnNames.get(c);
			TypeInfo ti = columnTypes.get(c);

			try {
				value = deserializeField(fields[c], ti);
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


	private Object deserializeField(String val, TypeInfo ti) {
		Object value;
		// Get type-safe values
		if (SerDeStringUtils.isNull(val)) {
			value = null;
		} else if (ti.getTypeName().equalsIgnoreCase(
				Constants.DOUBLE_TYPE_NAME)) {
			value = SerDeStringUtils.getDouble(val);
		} else if (ti.getTypeName().equalsIgnoreCase(
				Constants.BIGINT_TYPE_NAME)) {
			value = SerDeStringUtils.getLong(val);
		} else if (ti.getTypeName().equalsIgnoreCase(
				Constants.INT_TYPE_NAME)) {
			value = SerDeStringUtils.getInt(val);
		} else if (ti.getTypeName().equalsIgnoreCase(
				Constants.TINYINT_TYPE_NAME)) {
			value = Byte.valueOf(SerDeStringUtils.getString(val));
		} else if (ti.getTypeName().equalsIgnoreCase(
				Constants.FLOAT_TYPE_NAME)) {
			value = Float.valueOf(SerDeStringUtils.getString(val));
		} else if (ti.getTypeName().equalsIgnoreCase(
				Constants.BOOLEAN_TYPE_NAME)) {
			value = SerDeStringUtils.getBoolean(val);
		} else if (ti.getTypeName().equalsIgnoreCase(
				Constants.STRING_TYPE_NAME)) {
			value = SerDeStringUtils.getString(val);
		} else if (ti.getTypeName()
				.startsWith(Constants.LIST_TYPE_NAME)) {
			// Parse this 
			String[] newarr = val.split(addSepChar);
			value = newarr;

		} else {
			// Unknown type but there is a value
			value = val;
		}
		return value;
	}

    // Reusable objects (it's just text after all)
    Text serialized = new Text();
    StringBuilder sb = new StringBuilder();
    
	@Override
	public Writable serialize(Object o, ObjectInspector oi)
			throws SerDeException {

        StructObjectInspector soi = (StructObjectInspector) oi;
        List<? extends StructField> fields = soi.getAllStructFieldRefs();
        
        // for each field supplied by object inspector
        for (int i = 0; i < fields.size(); i++) {
            // for each column, set the correspondent event attribute
            StructField structField = fields.get(i);
			String fieldName = structField.getFieldName();
			Object fdata = soi.getStructFieldData(o, structField);
            try {
            	LOG.debug("Serializing column" + fieldName);
            	ObjectInspector currOI = structField.getFieldObjectInspector();
                // if null, the field is null
                if (o == null) {
                    return null;
                } else {

	                switch (currOI.getCategory()) {
	                    case PRIMITIVE: {
	                        PrimitiveObjectInspector poi = (PrimitiveObjectInspector) currOI;
	                        switch (poi.getPrimitiveCategory()) {
	                            case VOID: {
	                                break;
	                            }
	                            case BOOLEAN: {
	                                BooleanObjectInspector boi = (BooleanObjectInspector) poi;
	                                boolean v = ((BooleanObjectInspector) poi).get(fdata);
	                                sb.append(Boolean.toString(v));
	                                sb.append(";");
	                                break;
	                            }
	                            case BYTE: {
	                                ByteObjectInspector boi = (ByteObjectInspector) poi;
	                                byte v = boi.get(fdata);
	                                sb.append(String.valueOf(v));
	                                sb.append(";");
	                                break;
	                            }
	                            case SHORT: {
	                                ShortObjectInspector spoi = (ShortObjectInspector) poi;
	                                short v = spoi.get(fdata);
	                                sb.append(Short.toString(v));
	                                sb.append(";");
	                                break;
	                            }
	                            case INT: {
	                                IntObjectInspector ioi = (IntObjectInspector) poi;
	                                int v = ioi.get(fdata);
	                                sb.append(Integer.toString(v));
	                                sb.append(";");
	                                break;
	                            }
	                            case LONG: {
	                                LongObjectInspector loi = (LongObjectInspector) poi;
	                                long v = loi.get(fdata);
	                                sb.append(Long.toString(v));
	                                sb.append(";");
	                                break;
	                            }
	                            case FLOAT: {
	                                FloatObjectInspector foi = (FloatObjectInspector) poi;
	                                float v = foi.get(fdata);
	                                sb.append(Float.toString(v));
	                                sb.append(";");
	                                break;
	                            }
	                            case DOUBLE: {
	                                DoubleObjectInspector doi = (DoubleObjectInspector) poi;
	                                double v = doi.get(fdata);
	                                sb.append(Double.toString(v));
	                                sb.append(";");
	                                break;
	                            }
	                            case STRING: {
	                                Text t = ((StringObjectInspector) poi).getPrimitiveWritableObject(fdata);
	                                sb.append(t.toString());
	                                sb.append(";");
	                                break;
	                            }
	                            default: {
	                                throw new RuntimeException("Unrecognized type: " + poi.getPrimitiveCategory());
	                            }
	                        }
	                        break;
	                    }
	                    case LIST:
	                    case MAP:
	                    case STRUCT: {
	                        throw new RuntimeException("Complex types not supported : " + currOI.getCategory());
	                    }
	                    default: {
	                        throw new RuntimeException("Unrecognized type: " + currOI.getCategory());
	                    }
	                }
	                
                }
            } catch (EventException ex) {
                LOG.debug("Cannot set field " + fieldName, ex);
                throw new SerDeException("Can't set field " + fieldName
                        + " in SerDe serialize", ex);
            }
        }
        serialized.set(sb.toString());
        return serialized;
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
