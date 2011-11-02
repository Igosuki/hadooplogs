package load.model;

import java.util.LinkedList;

/**
 * Api class
 * Event exceptions
 * @author g_ba
 *
 */
public class EventException extends RuntimeException {
	
	/**
	 * Serial
	 */
	private static final long serialVersionUID = 1158139375961487402L;

	LinkedList<String> messages = new LinkedList<String>();
	
	public EventException() {
		super();
	}
	
	public EventException(String message) {
		super(message);
		
	}
	
	public void addMessage(String message) {
		messages.add(message);
	}
	
	public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("Event exceptions {\n");
        for (String s : messages) {
               buf.append(s).append("\n");
        }
        buf.append("\n}");
        return buf.toString();
    }

	public boolean hasExceptions() {
		return !messages.isEmpty();
	}
}
