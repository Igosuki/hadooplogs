package load.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * Api class
 * Basic event template
 * @author g_ba
 *
 */
public class EventTemplate {

	protected Map<String, Map<String, EventType>> templateTypes = null;
	
	protected static Map<String, EventType> knownTypes = null;
	
	static {
		initknown(); 
	}
	
	protected ArrayList<String> headers = new ArrayList<String>();
	
	char[] separatorsCache = new char[255];
	
	public EventTemplate(char[] separators) {
		templateTypes = new HashMap<String, Map<String, EventType>>();
		if (separators != null) {
			for (char c : separators) {
				separatorsCache[(int) c] = c;
			}
		}
	}
	
	private static void initknown() {
		knownTypes.put("string", new EventType("string"));
	}
	
	public static Boolean isKnownType(String typeName) {
		return knownTypes.containsKey(typeName);
	}
	
	public boolean checkEventType(String type) {
		return templateTypes.containsKey(type);
	}

	public boolean checkKeyExists(String eventType, String key) {
		return templateTypes.get(eventType).containsKey(key);
	}

	public EventType getEventTypeFrom(String eventType, String key) {
		return templateTypes.get(eventType).get(key);
	}
	
	public Map<String, EventType> getEventTypes(String eventType) {
		return templateTypes.get(eventType);
	}
	
	public boolean checkValueType(Object value, String eventType, String key) {
        EventType expected = getEventTypeFrom(eventType, key);
        EventType bt = EventType.eventTypeFromVal(value);
        return expected == bt;
	}
	
	public boolean isSeparator(char c) {
		return separatorsCache[(int)c] == 0;
	}
	
	public void parseHeader(String header, String sep) {
		StringTokenizer tokenizer = new StringTokenizer(header, sep);
		while(tokenizer.hasMoreTokens()) {
			headers.add(tokenizer.nextToken());
		}
	}
	
	public String getHeaderName(int index) {
		return headers.get(index);
	}
	
	public void setHeader(String header) {
		parseHeader(header, ";");
	}
}
