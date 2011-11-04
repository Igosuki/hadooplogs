package load.serde;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.service.HiveClient;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;

public class SerdeMarshallerTest {

	
	@Test
	public void testMarshall() {
		TSocket transport = null;
		try {
			transport = new TSocket("sep347", 10000);
			TBinaryProtocol protocol = new TBinaryProtocol(transport);
			HiveClient client = new HiveClient(protocol);
			transport.open();
			System.out.println(String.valueOf(client.aliveSince()));
			client.send_execute("drop table testtablejavaclient");
			closeThenOpen(transport);
			client.send_execute("create table testtablejavaclient (key int, value string)");
			closeThenOpen(transport);
			List<String> get_all_tables = client.get_all_tables("default");
			for (String string : get_all_tables) {
				System.out.print(string.concat(","));
			}
			closeThenOpen(transport);
			StringBuilder query = new StringBuilder();
			query.append("CREATE EXTERNAL TABLE IF NOT EXISTS my_table (");
			query.append("field1 string, field2 int, field3 string, field4 double");
			query.append(")");
			query.append("ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.JsonSerde'");
			query.append("LOCATION '/home/sesame/src/hivetests/'");
			client.send_execute(query.toString());
			closeThenOpen(transport);
			Table table = client.get_table("default", "my_table");
			table.getDbName();
		} catch (TTransportException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		} catch (MetaException e) {
			e.printStackTrace();
		} catch (NoSuchObjectException e) {
			e.printStackTrace();
		} finally {
			if(transport != null) {
				transport.close();
			}
		}
	}

	private void closeThenOpen(TSocket transport) throws TTransportException {
		transport.close();
		transport.open();
	}
}
