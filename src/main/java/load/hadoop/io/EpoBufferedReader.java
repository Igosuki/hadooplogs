package load.hadoop.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;

import load.model.EpoBaseEvent;
import load.model.EventFactory;

public class EpoBufferedReader extends BufferedReader {

	private EventFactory ef = null;
	
	public EpoBufferedReader(String fileName, Reader reader) {
		super(reader);
		try {
			ef = new EventFactory(fileName, readLine());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	 
	
	public EpoBaseEvent readEvent() {
		if(ef == null) {
			ef = new EventFactory(null, null);
		}
		// Instead of using a buffered reader to read lines, we can read the bytes and just stop at a new line or a predetermined line breaker
		return ef.createEvent(false);
	}

}
