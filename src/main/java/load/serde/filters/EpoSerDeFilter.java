package load.serde.filters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.naming.directory.NoSuchAttributeException;

import load.hadoop.writable.EpoWritable;
import load.model.EpoBaseEvent;
import load.model.EventException;
import load.model.EventFactory;
import load.model.EventTemplate;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
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
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class EpoSerDeFilter implements SerDe {

	 public static final Log LOG = LogFactory.getLog(EpoSerDeFilter.class.getName());
	    List<String> columnNames;
	    List<TypeInfo> columnTypes;
	    TypeInfo rowTypeInfo;
	    ObjectInspector rowObjectInspector;
	    boolean[] columnSortOrderIsDesc;
	    // holds the results of deserialization
	    ArrayList<Object> row;
	    EventTemplate template;
	    Map<String, List<FieldAndPosition>> fieldsForEpoBaseEventName =
	            new HashMap<String, List<FieldAndPosition>>();
	    String allEpoBaseEventName; // in case file has one type of EpoBaseEvent only

	    /**
	     * Prepares for serialization/deserialization, collecting the column
	     * names and mapping them to the LWES attributes they refer to.
	     * 
	     * @param conf
	     * @param props
	     * @throws SerDeException
	     */
	    @Override
	    public void initialize(Configuration conf, Properties props)
	            throws SerDeException {
	        LOG.debug("initialize, logging to " + EpoBaseEvent.class.getName());

	        // Get column names and sort order
	        String columnNameProperty = props.getProperty("columns");
	        String columnTypeProperty = props.getProperty("columns.types");
	        if (columnNameProperty.length() == 0) {
	            columnNames = new ArrayList<String>();
	        } else {
	            columnNames = Arrays.asList(columnNameProperty.split(","));
	        }
	        if (columnTypeProperty.length() == 0) {
	            columnTypes = new ArrayList<TypeInfo>();
	        } else {
	            columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
	        }
	        assert (columnNames.size() == columnTypes.size());

	        for (String s : props.stringPropertyNames()) {
	            LOG.debug("Property: " + s + " value " + props.getProperty(s));
	        }

	        if (props.containsKey("epo.event_name")) {
	            allEpoBaseEventName = props.getProperty("epo.event_name");
	        }

	        // Create row related objects
	        rowTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
	        //rowObjectInspector = (StructObjectInspector) TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);
	        rowObjectInspector = (StructObjectInspector) TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(rowTypeInfo);
	        row = new ArrayList<Object>(columnNames.size());

	        for (int i = 0; i < columnNames.size(); i++) {
	            row.add(null);
	        }

	        // Get the sort order
	        String columnSortOrder = props.getProperty(Constants.SERIALIZATION_SORT_ORDER);
	        columnSortOrderIsDesc = new boolean[columnNames.size()];
	        for (int i = 0; i < columnSortOrderIsDesc.length; i++) {
	            columnSortOrderIsDesc[i] = (columnSortOrder != null && columnSortOrder.charAt(i) == '-');
	        }
	        EventFactory ef = new EventFactory();
	        this.template = ef.createTemplate(allEpoBaseEventName); 
	        // take each column and find what it maps to into the EpoBaseEvent list
	        int colNumber = 0;
	        for (String columnName : columnNames) {
	            String fieldName;
	            String EpoBaseEventName;

	            if (!props.containsKey(columnName) && allEpoBaseEventName == null) {
	                LOG.warn("Column " + columnName
	                        + " is not mapped to an EpoBaseEventName:field through SerDe Properties");
	                continue;
	            } else if (allEpoBaseEventName != null) {
	                // no key, but in a single-type EpoBaseEvent file
	                EpoBaseEventName = allEpoBaseEventName;
	                fieldName = columnName;
	            } else {
	                // if key is there
	                String fullEpoBaseEventField = props.getProperty(columnName);
	                String[] parts = fullEpoBaseEventField.split("::");

	                // we are renaming the column
	                if (parts.length < 1 || (parts.length == 1 && allEpoBaseEventName != null)) {
	                    LOG.warn("Malformed EpoBaseEventName::Field " + fullEpoBaseEventField);
	                    continue;
	                } else if (parts.length == 1 && allEpoBaseEventName != null) {
	                    // adds the name. We're not specifying the EpoBaseEvent.
	                    fieldName = parts[0];
	                    EpoBaseEventName = allEpoBaseEventName;
	                } else {
	                    fieldName = parts[parts.length - 1];
	                    EpoBaseEventName = fullEpoBaseEventField.substring(0, fullEpoBaseEventField.length() - 2 - fieldName.length());
	                }
	                LOG.debug("Mapping " + columnName + " to EpoBaseEventName " + EpoBaseEventName
	                        + ", field " + fieldName);
	            }

	            if (!fieldsForEpoBaseEventName.containsKey(EpoBaseEventName)) {
	                fieldsForEpoBaseEventName.put(EpoBaseEventName, new LinkedList<FieldAndPosition>());
	            }

	            fieldsForEpoBaseEventName.get(EpoBaseEventName).add(new FieldAndPosition(fieldName, colNumber));
	            colNumber++;
	        }

	    }

	    /**
	     * Deserializes a single EpoBaseEvent, storing its fields into the row object
	     * @param ev
	     */
	    @Override
	    public Object deserialize(Writable w) throws SerDeException {
	        byte[] bytes = null;

	        LOG.debug("Deserialize called.");

	        EpoBaseEvent ev = null;
	        try {
	           if (w instanceof EpoWritable) {
	                EpoWritable ew = (EpoWritable) w;
	                ev = ew.getEvent();
	            } else {
	                throw new SerDeException(getClass().toString()
	                        + ": expects either BytesWritable or Text object!");
	            }
	            LOG.debug("Got bytes: " + bytes);
	        } catch (EventException ex) {
	            throw new SerDeException(ex);
	        }

	        if (this.fieldsForEpoBaseEventName.containsKey(ev.getEventType())) {
	            for (FieldAndPosition fp : fieldsForEpoBaseEventName.get(ev.getEventType())) {

	                TypeInfo type = columnTypes.get(fp.getPosition());

	                LOG.debug("Deserializing " + columnNames.get(fp.getPosition()));
	                try {
	                    row.set(fp.getPosition(),
	                            deserialize_column(ev, type, fp,
	                            row.get(fp.getPosition()))); // reusable object
	                } catch (IOException ex) {
	                    row.set(fp.getPosition(), null);
	                    Logger.getLogger(EpoSerDeFilter.class.getName()).log(Level.SEVERE, null, ex);
	                    continue;
	                }
	            }
	        }
	        return row;
	    }

	    /**
	     * Takes the parsed EpoBaseEvent, and maps its content to columns.
	     * @param ev
	     * @param type
	     * @param invert
	     * @param reuse
	     * @return
	     * @throws IOException
	     */
	    static Object deserialize_column(EpoBaseEvent ev, TypeInfo type,
	            FieldAndPosition fp, Object reuse) throws IOException {
	        LOG.debug("Deserializing column " + fp.getField());
	        // Is this field a null?
	        String fieldName = fp.getField();

	        // if field is not there or is not set, return null.
	        // isSet doesn't throw AttributeNotExists exception.
	        if (!ev.isSet(fieldName)) {
	            return null;
	        }
	        switch (type.getCategory()) {
			    case PRIMITIVE: {
			        PrimitiveTypeInfo ptype = (PrimitiveTypeInfo) type;
			        switch (ptype.getPrimitiveCategory()) {
			            case VOID: {
			                return null;
			            }
			            case BOOLEAN: {
			                BooleanWritable r = reuse == null ? new BooleanWritable() : (BooleanWritable) reuse;
			                r.set((Boolean) ev.get(fieldName));
			                return r;
			            }
			            case SHORT: {
			                ShortWritable r = reuse == null ? new ShortWritable() : (ShortWritable) reuse;
			                r.set((Short) ev.get(fieldName));
			                return r;
			            }
			            case INT: {
			                IntWritable r = reuse == null ? new IntWritable() : (IntWritable) reuse;
			                r.set((Integer) ev.get(fieldName));
			                return r;
			            }
			            case LONG: {
			                LongWritable r = reuse == null ? new LongWritable() : (LongWritable) reuse;
			                r.set((Long) ev.get(fieldName));
			                return r;
			            }
			            case FLOAT: {
			                FloatWritable r = reuse == null ? new FloatWritable() : (FloatWritable) reuse;
			                r.set(Float.parseFloat((String) ev.get(fieldName)));
			                return r;
			            }
			            case DOUBLE: {
			                DoubleWritable r = reuse == null ? new DoubleWritable() : (DoubleWritable) reuse;
			                r.set(Double.parseDouble((String) ev.get(fieldName)));
			                return r;
			            }
			            case STRING: {
			                Text r = reuse == null ? new Text() : (Text) reuse;
			                r.set(ev.get(fieldName).toString());
			                return r;
			            }
			            default: {
			                throw new RuntimeException("Unrecognized type: " + ptype.getPrimitiveCategory());
			            }
			        }
			    }
			    case LIST:
			    case MAP:
			    case STRUCT: {
			        throw new IOException("List, Map and Struct not supported in LWES");
			    }
			    default: {
			        throw new RuntimeException("Unrecognized type: " + type.getCategory());
			    }
			}

	    }

	    @Override
	    public ObjectInspector getObjectInspector() throws SerDeException {
	        LOG.debug("Epo SerDe : getObjectInspector()");
	        return rowObjectInspector;
	    }

	    @Override
	    public Class<? extends Writable> getSerializedClass() {
	        LOG.debug("Epo SerDe : getSerializedClass()");
	        return EpoWritable.class;
	    }
	    /**
	     * Serializes an EpoBaseEvent to a writable object that can be handled by hadoop
	     * @param obj
	     * @param inspector
	     * @return
	     * @throws SerDeException
	     */

	    // buffers used to serialize
	    // we reused these objects since we don't want to create/destroy
	    // a huge number of objects in the JVM
	    EpoWritable serialized = new EpoWritable();
	    EpoBaseEvent serializeev = null;

	    @Override
	    public Writable serialize(Object obj, ObjectInspector inspector) throws SerDeException {
	        LOG.debug("Serializing: " + obj.getClass().getCanonicalName());

	        try {
	            if(serializeev == null)
	                serializeev = new EpoBaseEvent(false, null, allEpoBaseEventName);
	        } catch (EventException ex) {
	            LOG.debug("Cannot create EpoBaseEvent ", ex);
	            throw new SerDeException("Can't create EpoBaseEvent in SerDe serialize", ex);
	        }

	        StructObjectInspector soi = (StructObjectInspector) inspector;
	        List<? extends StructField> fields = soi.getAllStructFieldRefs();

	        // for each field supplied by object inspector
	        for (int i = 0; i < fields.size(); i++) {
	            // for each column, set the correspondent EpoBaseEvent attribute
	            String fieldName = fields.get(i).getFieldName();
	            try {
	                serialize_column(serializeev,
	                        columnNames.get(i),
	                        soi.getStructFieldData(obj, fields.get(i)), //data for Struct Field
	                        fields.get(i).getFieldObjectInspector());  // object inspector
	            } catch (EventException ex) {
	                LOG.debug("Cannot set field " + fieldName, ex);
	                throw new SerDeException("Can't set field " + fieldName
	                        + " in SerDe serialize", ex);
	            }
	        }
	        // if u ask me, this should be BytesWritable
	        // but apparently hive gets confused
	        serialized.setEvent(serializeev);
	        
	        return serialized;

	    }

	    private void serialize_column(EpoBaseEvent ev, String f, Object o, ObjectInspector oi)
	            throws EventException {
	        LOG.debug("Serializing column" + f);

	        // just don't set the field.
	        if (o == null) {
	            return;
	        }

	        switch (oi.getCategory()) {
	            case PRIMITIVE: {
	                PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
	                switch (poi.getPrimitiveCategory()) {
	                    case VOID: {
	                        return;
	                    }
	                    case BOOLEAN: {
	                        BooleanObjectInspector boi = (BooleanObjectInspector) poi;
	                        boolean v = ((BooleanObjectInspector) poi).get(o);
	                        ev.set(f, v);
	                        return;
	                    }
	                    case BYTE: {
	                        ByteObjectInspector boi = (ByteObjectInspector) poi;
	                        byte v = boi.get(o);
	                        // lwes doesn't have byte so we upcast
	                        ev.set(f, v);
	                        return;
	                    }
	                    case SHORT: {
	                        ShortObjectInspector spoi = (ShortObjectInspector) poi;
	                        short v = spoi.get(o);
	                        ev.set(f, v);
	                        return;
	                    }
	                    case INT: {
	                        IntObjectInspector ioi = (IntObjectInspector) poi;
	                        int v = ioi.get(o);
	                        ev.set(f, v);
	                        return;
	                    }
	                    case LONG: {
	                        LongObjectInspector loi = (LongObjectInspector) poi;
	                        long v = loi.get(o);
	                        ev.set(f, v);
	                        return;
	                    }
	                    case FLOAT: {
	                        FloatObjectInspector foi = (FloatObjectInspector) poi;
	                        float v = foi.get(o);
	                        ev.set(f, Float.toString(v));
	                        return;
	                    }
	                    case DOUBLE: {
	                        DoubleObjectInspector doi = (DoubleObjectInspector) poi;
	                        double v = doi.get(o);
	                        ev.set(f, Double.toString(v));
	                        return;
	                    }
	                    case STRING: {
	                        StringObjectInspector soi = (StringObjectInspector) poi;
	                        Text t = soi.getPrimitiveWritableObject(o);
	                        ev.set(f, (t != null ? t.toString() : null));
	                        return;
	                    }
	                    default: {
	                        throw new RuntimeException("Unrecognized type: " + poi.getPrimitiveCategory());
	                    }
	                }
	            }
	            case LIST:
	            case MAP:
	            case STRUCT: {
	                throw new RuntimeException("Complex types not supported in LWES: " + oi.getCategory());
	            }
	            default: {
	                throw new RuntimeException("Unrecognized type: " + oi.getCategory());
	            }
	        }
	    }

	    public static class FieldAndPosition {

	        String field;
	        int position;

	        public FieldAndPosition(String f, int p) {
	            field = f;
	            position = p;
	        }

	        public String getField() {
	            return field;
	        }

	        public void setField(String field) {
	            this.field = field;
	        }

	        public int getPosition() {
	            return position;
	        }

	        public void setPosition(int position) {
	            this.position = position;
	        }

	        @Override
	        public String toString() {
	            return "<" + field + "," + position + ">";
	        }
	    }

}
