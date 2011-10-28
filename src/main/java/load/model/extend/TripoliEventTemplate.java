package load.model.extend;

import java.util.HashMap;
import java.util.Map;

import load.model.EventTemplate;
import load.model.EventType;

public class TripoliEventTemplate extends EventTemplate {

	enum TripoliType {
		JOURNAL, CONFERENCE, INTERM;
	}
	
	//TODO : Change these init methods over to json or xml loading format
	private static char [] seps = {';'};
	public TripoliEventTemplate() {
		super(seps);
		this.templateTypes.put(TripoliType.JOURNAL.toString(), initJournalLoader());
		this.templateTypes.put(TripoliType.CONFERENCE.toString(), initJournalIn());
		this.templateTypes.put(TripoliType.INTERM.toString(), initJournalInterm());
	}
	

	/**
	 * Event type format for journal 'in' tripoli type
	 * @return the event type format map
	 */
	private Map<String, EventType> initJournalIn() {
		Map<String, EventType> type = new HashMap<String, EventType>();
		initTripoliType(type);
		return type;
	}
	
	/**
	 * Event type format for journal 'interm' tripoli type
	 * @return the event type format map
	 */
	private Map<String, EventType> initJournalInterm() {
		Map<String, EventType> type = new HashMap<String, EventType>();
		type.put("status", new EventType("string"));
		return type;
	}
	
	/**
	 * Event type format for journal 'loader' tripoli type
	 * @return the event type format map
	 */
	private Map<String, EventType> initJournalLoader() {
		Map<String, EventType> type = new HashMap<String, EventType>();
		initTripoliType(type);
		type.put("xp", new EventType("string"));
		return type;
	}
	
	private void initTripoliType(Map<String, EventType> type) {
		type.put("pubDocId", new EventType("string"));
		type.put("batchId", new EventType("string"));
		type.put("timestamp", new EventType("string"));
		type.put("deliveryInfo", new EventType("string"));
		type.put("flowType", new EventType("string"));
		type.put("provider", new EventType("string"));
	}
}
