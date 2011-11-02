package load.model;

import java.util.LinkedHashMap;

/**
 * Api class
 * Base event
 * @author g_ba
 *
 */
public class EpoBaseEvent {

	/** The values of this event **/
	private LinkedHashMap<String, Object> values;
	
	/** Is the event validating **/
	private boolean validating = true;

	/** Event type **/
	private String eventType = null;
	
	/** Event size **/
	private int size = 0;
	
	/** Associated event template **/
	private EventTemplate template = null;

	private int bytesStoreSize = 0;
	
	public EpoBaseEvent() {
		this(true);
	}
	
	public EpoBaseEvent(Boolean validating) {
		this(true, new EventTemplate(new char[0]));
	}
	
	public EpoBaseEvent(Boolean validating, EventTemplate template) {
		this(true, template, null);
	}
	
	public EpoBaseEvent(Boolean validating, EventTemplate template, String eventType) {
		this.validating = validating;
		this.template = template;
		this.eventType = eventType;
	}
	
	public EpoBaseEvent(String eventLine, Boolean validating, EventTemplate template, String eventType) {
		this.validating = validating;
		this.template = template;
		this.eventType = eventType;
		parseEvent(eventLine);
	}
	
	public void parseEvent(String eventLine) {
		int headercount = 0;
		int lastIndex = 0;
		for (int i = 0; i < eventLine.length(); i++) {
			if(template.isSeparator(eventLine.charAt(i))){
				//TODO : Here we can use the initialized types of the template to initiate values 
				set(template.getHeaderName(headercount), eventLine.substring(lastIndex, i));
				headercount++;
				lastIndex = i;
			}
		}
	}
	
	public void set(String name, Object value) {
		try {
			if(isValidating() && template.checkKeyExists(eventType, name)
					&& template.checkValueType(get(name), eventType, name)) {
				//this.size += type size of value's type token id 
				this.values.put(name, value);
				bytesStoreSize += getByteSize(value);
			}
		} catch (EventException ev) {
			System.out.println("Invalid event " + ev.toString() + "\n");
		}
		
	}
	
	private int getByteSize(Object value) {
		if(value instanceof String) {
			return ((String) value).getBytes().length;
		}
		return 0;
	}

	public Object get(String key) {
		return this.values.get(key);
	}

	
	public boolean isValidating() {
		return this.validating;
	}
	
	/**
     * This method can be used to validate an event after it has been created.
	 * @return 
     *
     * @throws EventSystemException
     */
    public boolean validate() throws EventException {

        if (this.template == null) {
            throw new EventException("No template to validate against.");
        }
        if (!template.checkEventType(eventType)) {
        	throw new EventException("Template does not support event");
        }
        EventException ex = new EventException();
        for (String key : values.keySet()) {
            if (!template.checkKeyExists(eventType, key)) {
            	ex.addMessage("Attribute " + key + " does not exist for event " + eventType);
                continue;
            }
            if (!template.checkValueType(get(key), eventType, key)) {
                ex.addMessage("Wrong type '" + eventType);
            }
        }
        if (ex.hasExceptions()) {
            throw ex;
        }
        return true;
    }
    
    public EpoBaseEvent copy()  {
    	EpoBaseEvent evt = new EpoBaseEvent(isValidating(), template, eventType);
        for (String key : values.keySet()) {
            Object value = values.get(key);
            evt.set(key, value);
        }
        return evt;
    }
    
    public String toString() {
    	StringBuilder s = new StringBuilder();
    	for (Object o : this.values.values()) {
			s.append(String.valueOf(o)).append('\t');
		}
    	return s.toString();
    }
    
    public byte[] serialize() {
    	byte[] bytes = new byte[bytesStoreSize];
    	int offset = 0;
    	for (Object o : this.values.values()) {
    		offset += getBytes(bytes, o, offset);
		}
    	return bytes;
    }

	private int getBytes(byte[] bytes, Object o, int offset) {
		if(o instanceof String) {
			byte[] stringBytes = ((String)o).getBytes();
	        int length = stringBytes.length;
	        if (length < 65535 && length >= 0) {
	            offset += length;
	            System.arraycopy(stringBytes, 0, bytes, offset, length);
	            return (length + 2);
	        }
		}
		return 0;
	}

	/**
	 * @return the eventType
	 */
	public String getEventType() {
		return eventType;
	}

	/**
	 * @param eventType the eventType to set
	 */
	public void setEventType(String eventType) {
		this.eventType = eventType;
	}

	/**
	 * @return the template
	 */
	public EventTemplate getTemplate() {
		return template;
	}

	/**
	 * @param template the template to set
	 */
	public void setTemplate(EventTemplate template) {
		this.template = template;
	}

	public boolean isSet(String fieldName) {
		return values.get(fieldName) != null;
	}
	
}

