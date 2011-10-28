package load.model;

import load.model.extend.TripoliEventTemplate;


/**
 * Api class
 * Basic event factory
 * @author g_ba
 *
 */
public class EventFactory {

	protected EventTemplate template = null;
	
	private String eventType = null; 
	
	public EventFactory() {
		
	}
	
	public EventFactory(String eventFileName, String header) {
		this.template = createTemplate(eventFileName);
		this.template.setHeader(header);
		eventType = getEventTypeFromFileName(eventFileName);
	}
	
	public EventTemplate createTemplate(String eventFileName) {
		if (eventFileName != null) {
			if (eventFileName.contains("conference")) {
				return new TripoliEventTemplate();
			}
		}
		return new EventTemplate(null);
	}

	private String getEventTypeFromFileName(String fileName) {
		if(fileName.contains("in") && !fileName.contains("interm")){
			return "in";
		} else if(fileName.contains("interm")){
			return "interm";
		} else if(fileName.contains("loader")){
			return "loader";
		}
		return null;
	}

	public EpoBaseEvent createEvent(boolean validate) {
		if (validate && this.template == null) {
            throw new EventException("Event template db not initialized");
        }
		return new EpoBaseEvent(validate, template, eventType);
	}
}
