package load;

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
}
