package load.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import load.hadoop.conf.ConfigurationContext;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.junit.Assert;
import org.junit.Test;

import de.odysseus.el.ExpressionFactoryImpl;
import de.odysseus.el.TreeValueExpression;
import de.odysseus.el.util.SimpleContext;

public class ParseContextTest {

	ExpressionFactoryImpl ef = new ExpressionFactoryImpl();
	
	@Test
	public void testConfigurationContextIn() {
		try {
			FileInputStream fis = new FileInputStream(new File("src/main/resources/properties/jobs/in.xml"));
			ConfigurationContext ctxt = new ConfigurationContext("2011-06-22_00-00-04_ieee.conference_in.csv", fis);
			ctxt.parseContent("week17a_18_5752414_5752449;IEEEcnf/week17a/18.zip;20110622_001238");
			System.out.println(ctxt.flush());
			ctxt.parseContent("week17a_2_30050_1106183;IEEEcnf/week17a/2.zip;20110622_001238");
			System.out.println(ctxt.flush());
		} catch (FileNotFoundException e) {
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testConfigurationContextInBig() {
		try {
			FileInputStream fis = new FileInputStream(new File("src/main/resources/properties/jobs/in.xml"));
			ConfigurationContext ctxt = new ConfigurationContext("2011-06-22_00-00-04_ieee.conference_in.csv", fis);
			LineReader lr = new LineReader(new FileInputStream(new File("src/main/resources/2011-06-22_00-00-04_ieee.conference_in.csv")));
			Text value = new Text();
			try {
				while(lr.readLine(value) > 0) {
					ctxt.parseContent(value.toString());
					System.out.println(ctxt.flush());
				}
			} catch (IOException e) {
				Assert.fail(e.getMessage());
			}
		} catch (FileNotFoundException e) {
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testConfigurationContextInTermBig() {
		try {
			FileInputStream fis = new FileInputStream(new File("src/main/resources/properties/jobs/interm.xml"));
			ConfigurationContext ctxt = new ConfigurationContext("2011-06-22_00-00-04_ieee.conference_interm_in.csv", fis);
			LineReader lr = new LineReader(new FileInputStream(new File("src/main/resources/2011-06-22_00-00-04_ieee.conference_interm_in.csv")));
			Text value = new Text();
			try {
				while(lr.readLine(value) > 0) {
					ctxt.parseContent(value.toString());
					System.out.println(ctxt.flush());
				}
			} catch (IOException e) {
				Assert.fail(e.getMessage());
			}
		} catch (FileNotFoundException e) {
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testConfigurationContextLoaderBig() {
		try {
			FileInputStream fis = new FileInputStream(new File("src/main/resources/properties/jobs/loader.xml"));
			ConfigurationContext ctxt = new ConfigurationContext("ieee.conference_week17b_loader.csv", fis);
			LineReader lr = new LineReader(new FileInputStream(new File("src/main/resources/ieee.conference_week17b_loader.csv")));
			Text value = new Text();
			try {
				while(lr.readLine(value) > 0) {
					ctxt.parseContent(value.toString());
					System.out.println(ctxt.flush());
				}
			} catch (IOException e) {
				Assert.fail(e.getMessage());
			}
		} catch (FileNotFoundException e) {
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testParse() {
		SimpleContext context = new SimpleContext();
		final String expression = "${pi}_${pitwo}";
		context.setVariable("pi", ef.createValueExpression("this", String.class));
		context.setVariable("pitwo", ef.createValueExpression("thistwo", String.class));
		TreeValueExpression e = ef.createValueExpression(context, expression, Object.class);
		PrintWriter out = new PrintWriter(System.out);
		e.dump(out);
		out.flush();
		System.out.println(e.isDeferred()); // true
		System.out.println(e.isLeftValue()); // false
		System.out.println(expression + " : " + e.getValue(context));
	}
	
	@Test
	public void testParseSpeed() {
		
		SimpleContext context = new SimpleContext();
		final String expression = "${pi}_${pitwo}";
		context.setVariable("pi", ef.createValueExpression("this", String.class));
		context.setVariable("pitwo", ef.createValueExpression("thistwo", String.class));
		TreeValueExpression e = ef.createValueExpression(context, expression, Object.class);
		long now = System.nanoTime();
		System.out.println(expression + " : " + e.getValue(context));
		long then = System.nanoTime();
		System.out.println("Expression took " + (then - now) + " to be resolved " );
	}
	
	@Test
	public void testParseSpeedFake() {
		SimpleContext context = new SimpleContext();
		final String expression = "${pi}_${pitwo}";
		final String str = "\\$\\{pi\\}";
		final String strtwo = "\\$\\{pitwo\\}";
		long now = System.nanoTime();
		String newString = expression.replaceFirst(str, "this");
		String evennewer = newString.replaceFirst(strtwo, "thistwo");
		System.out.println(expression + " : " + evennewer);
		long then = System.nanoTime();
		System.out.println("Expression took " + (then - now) + " to be resolved " );
	}
	
	@Test
	public void testLinkedHMOrder() {
		for (int i = 0; i < 10; i++) {
			Map<String, String> manualFileNameConf = new LinkedHashMap<String, String>();
			manualFileNameConf.put("interm_in.csv", "interm");
			manualFileNameConf.put("_in.csv", "in");
			manualFileNameConf.put("_loader.csv", "loader");
			for (Entry<String, String> entry : manualFileNameConf.entrySet()) {
				System.out.println("Try : " + entry.getKey());
				if ("2011-06-22_00-00-04_ieee.conference_interm_in.csv".endsWith(entry.getKey())) {
					System.out.println(entry.getValue());
					break;
				}
			}
		}
		
	}
}
