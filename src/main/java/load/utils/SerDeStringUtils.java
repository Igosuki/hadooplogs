package load.utils;

import load.model.EventException;

public final class SerDeStringUtils {

	private SerDeStringUtils() { }
	
	public static final boolean isNull(String field) {
		return !("".equals(field) || "null".equals(field)); 
	}
	
	// Parsing methods use wrapper classes only for now 
	public static final double getDouble(String field) {
		try {
			return Double.valueOf(field);
		} catch (NumberFormatException e) {
			throw new EventException("Field was not a double format");
		} 
	}
	
	
	public static final int getInt(String field) {
		try {
			return Integer.valueOf(field);
		} catch (NumberFormatException e) {
			throw new EventException("Field was not an integer format");
		} 
	}

	public static String getString(String field) {
		return field;	
	}

	public static boolean getBoolean(String field) {
		return Boolean.valueOf(field);
	}

	public static long getLong(String field) {
		try {
			return Long.valueOf(field);
		} catch (NumberFormatException e) {
			throw new EventException("Field was not an integer format");
		}
	}
	
	
}
