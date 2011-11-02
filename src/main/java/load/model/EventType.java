package load.model;


/**
 * This class is used to hold the meta about an event type
 * @author g_ba
 *
 */
public class EventType {

	private String typeName;
	
	private NativeTypes typeEnum;
	
	
	public EventType(String typeName, NativeTypes typeEnum) {
		this.typeName = typeName;
		this.typeEnum = typeEnum;
	}
	
	public EventType(String typeName) {
		this.typeName = typeName;
		for (NativeTypes ev : NativeTypes.values()) {
			if(ev.getName().equals(typeName)) {
				typeEnum = ev;
				break;
			}
		}
	}
	
	/**
	 * @return the typeName
	 */
	public String getTypeName() {
		return typeName;
	}

	/**
	 * @param typeName the typeName to set
	 */
	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}

	public static EventType eventTypeFromVal(Object value) {
		if (value instanceof String) {
            return new EventType(NativeTypes.STRING.getName(), NativeTypes.STRING);
        }
        if (value instanceof Short) {
        	return new EventType(NativeTypes.SHORT.getName(), NativeTypes.SHORT);
        }
        if (value instanceof Integer) {
        	return new EventType(NativeTypes.INTEGER.getName(), NativeTypes.INTEGER);
        }
        if (value instanceof Long) {
        	return new EventType(NativeTypes.LONG.getName(), NativeTypes.LONG);
        }
        if (value instanceof Double) {
        	return new EventType(NativeTypes.DOUBLE.getName(), NativeTypes.DOUBLE);
        }
        if (value instanceof Boolean) {
        	return new EventType(NativeTypes.DOUBLE.getName(), NativeTypes.DOUBLE);
        }
        else {
            System.out.println("Unknown object class : "+value.getClass().getName());
            return null;
        }
	}
	
}
