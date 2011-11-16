package load;

import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;

public class GenericTests {

	@Test 
	public void testChars() {
		Character thechar = Character.valueOf('_');
		System.out.println((int)'_');
		char[] table = new char[255];
		System.out.println(table[(int)'a']);
		Assert.assertTrue(table[(int)'a'] == 0);
		System.out.println(thechar);
		Assert.assertTrue(thechar != -1); 
	}
	
	@Test
	public void testAccess() {
		
		long now1 = System.nanoTime();
		for (int i = 0; i < 300000000; i++) {
			final String name = getName();
			if (!StringUtils.isBlank(name)) {
				String name2 = new String(name);
			}
		}
		long then1 = System.nanoTime();
		final long l = then1-now1;
		System.out.println("Access with ref replica : " + l);
		long now2 = System.nanoTime();
		for (int i = 0; i < 300000000; i++) {
			if(!StringUtils.isBlank(getName())) {
				String name2 = new String(getName());
			}
		}
		long then2 = System.nanoTime();
		final long l2 = then2-now2;
		System.out.println("Access with method invoke : " + l2);
		System.out.println((double)l/(double)l2);
	}
	
	public String name = "name";
	
	public String getName() {
		return this.name;
	}
}
