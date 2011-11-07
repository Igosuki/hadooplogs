package load.serde;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import load.serde.filters.CsvSerDe;
import load.utils.Constants.TableProps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.json.JSONException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CsvSerdeTest {

	public CsvSerdeTest() {
	}

	@BeforeClass
	public static void setUpClass() throws Exception {
	}

	@AfterClass
	public static void tearDownClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		initialize();
	}

	@After
	public void tearDown() {
	}

	static CsvSerDe instance;

	static public void initialize() throws Exception {
		System.out.println("initialize");
		instance = new CsvSerDe();
		Configuration conf = null;
		Properties tbl = new Properties();
		tbl.setProperty(TableProps.COLUMNS, "one,two,three");
		tbl.setProperty(TableProps.COLUMN_TYPES, "string,string,string");
		tbl.setProperty(TableProps.COLUMN_DEFAULTS, "one, two, three");
		instance.initialize(conf, tbl);
	}

	/**
	 * Test of deserialize method, of class CsvSerDe.
	 */
	@Test
	public void testDeserialize() throws Exception {
		System.out.println("deserialize");
		Writable w = new Text( "week17a_16_5753310_5720301;IEEEcnf/week17a/16.zip;20110622_001238");
		Object expResult = null;
		ArrayList<Object> result = (ArrayList<Object>) instance.deserialize(w);
		Assert.assertEquals(result.get(0), "week17a_16_5753310_5720301");
		Assert.assertEquals(result.get(1), "IEEEcnf/week17a/16.zip");

	}

	/**
	 * Test of getSerializedClass method, of class CsvSerDe.
	 */
	@Test
	public void testGetSerializedClass() {
		System.out.println("getSerializedClass");
		Class expResult = Text.class;
		Class result = instance.getSerializedClass();
		Assert.assertEquals(expResult, result);

	}

	/**
	 * Test of serialize method, of class CsvSerDe.
	 */
	@Test
	public void testSerialize() throws SerDeException, JSONException {
		System.out.println("serialize");
		ArrayList row = new ArrayList(3);

		List<ObjectInspector> lOi = new LinkedList<ObjectInspector>();
		List<String> fieldNames = new LinkedList<String>();

		row.add("week17a_16_5753310_5720301");
		fieldNames.add("one");
		lOi.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,
						ObjectInspectorFactory.ObjectInspectorOptions.JAVA));

		row.add("IEEEcnf/week17a/16.zip");
		fieldNames.add("two");
		lOi.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,
				ObjectInspectorFactory.ObjectInspectorOptions.JAVA));

		row.add("20110622_001238");
		fieldNames.add("three");
		lOi.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,
				ObjectInspectorFactory.ObjectInspectorOptions.JAVA));

		StructObjectInspector soi = ObjectInspectorFactory
				.getStandardStructObjectInspector(fieldNames, lOi);

		Object result = instance.serialize(row, soi);
		String exp = "week17a_16_5753310_5720301;IEEEcnf/week17a/16.zip;20110622_001238;";
		String res = result.toString();
		
		Assert.assertEquals(res, exp);

		System.out.println("Serialized to " + result.toString());

	}

}
