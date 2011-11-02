package load.hadoop.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import load.model.EpoBaseEvent;
import load.model.EventException;

import org.apache.hadoop.io.WritableComparable;

public class EpoWritable implements WritableComparable<EpoWritable> {

	private EpoBaseEvent event;
	
	@Override
	public void readFields(DataInput in) throws IOException{
		String readLine = in.readLine();
		final int length = in.readInt();
        try {
            EpoBaseEvent event2 = new EpoBaseEvent(false);
            event2.parseEvent(readLine);
			setEvent(event2);
        } catch (EventException ex) {
            throw new IOException("EventException", ex);
        }
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBytes(event.toString());
	}

	@Override
	public int compareTo(EpoWritable o) {
		Thread.dumpStack();
		return compare(getEvent(), o.getEvent());
	}

	@SuppressWarnings("unchecked")
	private static int compare(EpoBaseEvent a, EpoBaseEvent b) {
		int comp = 0;
		if(a.getEventType().equals(b.getEventType())) {
			if(a.getTemplate() != null && b.getTemplate() != null) {
				for (String name : a.getTemplate().getEventTypes(a.getEventType()).keySet()) {
					final Object av = a.get(name), bv = b.get(name);
					if (av==null&&bv==null) continue;
					if (av==null) return -1;
					if (bv==null) return 1;
					if ((av instanceof Comparable)&&(bv instanceof Comparable)) {
						comp = ((Comparable) av).compareTo(bv);
						if (comp!=0) return comp;
					}
				}
			}
		}
		return comp;
	}

	@Override
	public String toString() {
		return event==null ? "null" : event.toString();
	}
	
	/**
	 * @return the event
	 */
	public EpoBaseEvent getEvent() {
		return event;
	}

	/**
	 * @param event the event to set
	 */
	public void setEvent(EpoBaseEvent event) {
		this.event = event;
	}
	
}
