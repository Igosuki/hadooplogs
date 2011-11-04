package load.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Assert;
import org.junit.Test;

public class HiveJdbcClientTest {

	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";

	/**
	 * @param args
	 */
	@Test
	public void testJdbcPut() throws SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		Connection con = DriverManager.getConnection(
				"jdbc:hive://sep347.sesame.infotel.com:10000/default", "", "");
		Statement stmt = con.createStatement();
		String tableName = "testhivedrivertable";
		//drop table if it exists
		stmt.executeQuery("drop table " + tableName);
		//create table with key, value
		ResultSet res = stmt.executeQuery("create table if not exists " + tableName 
				+ " (key int, value string, value2 string, value3 string, value4 string, value5 string)");
		// show tables
		System.out.println(res);
		String sql = "show tables '" + tableName + "'";
		printrun(sql);
		// describe table
		sql = "describe " + tableName;
		printrun(sql);
		res = execsql(stmt, sql);
		while (res.next()) {
			System.out.println(res.getString(1) + "\t" + res.getString(2));
		}
		Assert.assertTrue(res.getString(1).length() > 0);
		Assert.assertTrue(res.getString(2).length() > 0);
		StringBuilder query = new StringBuilder();
		query.append("CREATE EXTERNAL TABLE IF NOT EXISTS my_table (");
		query.append("field1 string, field2 int, field3 string, field4 double");
		query.append(")");
		query.append("ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.JsonSerde'");
		query.append("LOCATION '/home/sesame/src/hivetests/'");
		stmt.executeQuery(query.toString());
		res = execsql(stmt, "select * from my_table limit 10");
//		// load data into table
//		// NOTE: filepath has to be local to the hive server
//		// NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line
//		String filepath = "./src/main/resources/testhivedriver.txt";
//		sql = "load data inpath '" + filepath + "' into table "
//				+ tableName;
//		printrun(sql);
//		res = execsql(stmt, sql);

		// select * query
		sql = "select * from my_table" ;
		printrun(sql);
		res = execsql(stmt, sql);
		while (res.next()) {
			while(res.next()) {
				for (int i = 1; i <= 4; i++) {
					System.out.print(String.valueOf(res.getString(i)) + "\t");
				}
				System.out.print("\n");
			}
		}
		// regular hive query
		sql = "select count(1) from " + tableName;
		printrun(sql);
		res = execsql(stmt, sql);
		while (res.next()) {
			System.out.println(res.getString(1));
		}
		
		StringBuilder queryEpo = new StringBuilder();
		queryEpo.append("CREATE EXTERNAL TABLE IF NOT EXISTS my_table_epo (");
		queryEpo.append("field1 string, field2 int, field3 string, field4 double");
		queryEpo.append(")");
		queryEpo.append("ROW FORMAT SERDE 'load.serde.filters.EpoSerDeFilter'");
		queryEpo.append("LOCATION '/home/sesame/src/hivetests/'");
		stmt.executeQuery(queryEpo.toString());
		res = execsql(stmt, "select * from my_table_epo limit 10");
	}

	private static ResultSet execsql(Statement stmt, String sql)
			throws SQLException {
		return stmt.executeQuery(sql);
	}

	private static void printrun(String sql) {
		System.out.println("Running: " + sql);
	}

}
