package load.conf;

import java.io.PrintWriter;

import org.junit.Test;

import de.odysseus.el.ExpressionFactoryImpl;
import de.odysseus.el.TreeValueExpression;
import de.odysseus.el.util.SimpleContext;

public class ParseContextTest {

	ExpressionFactoryImpl ef = new ExpressionFactoryImpl();
	
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
	
	
}
