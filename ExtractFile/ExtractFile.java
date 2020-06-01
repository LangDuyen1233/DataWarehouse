package week3;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class ExtractFile {
//	String jdbcURL_1 = "jdbc:mysql://localhost:3306/datawarehouse?user=root&password=Pass&useUnicode=true&characterEncoding=UTF-8";
	String jdbcURL_1 = "jdbc:mysql://localhost/datawarehouse?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC&characterEncoding=UTF-8";
	String userName_1 = "root";
	String password_1 = "";

	String jdbcURL_2 = "jdbc:mysql://localhost/datacopy?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC&characterEncoding=UTF-8";
	String userName_2 = "root";
	String password_2 = "";

	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		ExtractFile ex = new ExtractFile();
		String urlFile = "src\\week3\\thongtincanhan.txt";
		ex.load(urlFile);
		ex.copy("datawarehouse", "datacopy", "information");
	}

	public void load(String excelFile) throws ClassNotFoundException, SQLException, IOException {

		Connection connect = DBConnection.getConnection(jdbcURL_1, userName_1, password_1);
		System.out.println("Connect DB Successfully :)");

		BufferedReader lineReader = new BufferedReader(new FileReader(excelFile));
		String lineText = null;

		int count = 0;
		String sql;

		lineText = lineReader.readLine();
		String[] fields = lineText.split("\\|");
		System.out.println(fields.length);
		System.out.println(lineText);
		// create table
		sql = "CREATE table information(" + fields[0] + " CHAR(50)," + fields[1] + " CHAR(50)," + fields[2]
				+ " CHAR(50)," + fields[3] + " CHAR(50)," + fields[4] + " CHAR(50)," + fields[5] + " CHAR(50))";
		System.out.println(sql);
		PreparedStatement preparedStatement = connect.prepareStatement(sql);
		preparedStatement.execute();
		System.out.println("Create table Successfully :)");

		// skip header line
		String query = "INSERT INTO information VALUES(?, ?, ?, ?, ?, ?)";
		PreparedStatement pre = connect.prepareStatement(query);
		while ((lineText = lineReader.readLine()) != null) {
			String[] data = lineText.split("\\|");
			String id = data[0];
			String name = data[1];
			String gender = data[2];
			String phone = data[3];
			String job = data[4];
			String address = data[5];
			pre.setString(1, id);
			pre.setString(2, name);
			pre.setString(3, gender);
			pre.setString(4, phone);
			pre.setString(5, job);
			pre.setString(6, address);
			pre.execute();
		}
	}

	public void copy(String database1, String database2, String nameDB) throws ClassNotFoundException, SQLException {
		Connection connectionDB1 = DBConnection.getConnection(jdbcURL_1, userName_1, password_1);
		System.out.println("c1 ok");
		Connection connectionDB2 = DBConnection.getConnection(jdbcURL_2, userName_2, password_2);
		System.out.println("c2 ok");

		ResultSet rs;
		Statement stmt = connectionDB1.createStatement();
		rs = stmt.executeQuery("SELECT * FROM information");
		ResultSetMetaData md = (ResultSetMetaData) rs.getMetaData();
		int counter = md.getColumnCount();
		String colName[] = new String[counter];
		System.out.println("The column names are as follows:");
		for (int loop = 1; loop <= counter; loop++) {
			colName[loop - 1] = md.getColumnLabel(loop);
//			sqlCreateTable += colName[loop - 1] + " CHAR(50),";
		}
		String sqlCreateTable = "CREATE table " + nameDB + "copy" + "(" + colName[0] + " VARCHAR(15)," + colName[1]
				+ " CHAR(50)," + colName[2] + " CHAR(50)," + colName[3] + " CHAR(50)," + colName[4] + " CHAR(50),"
				+ colName[5] + " CHAR(50))";
//		sqlCreateTable += ")";

		System.out.println(sqlCreateTable);
		PreparedStatement p = connectionDB2.prepareStatement(sqlCreateTable);
		p.execute();

//		COPY 
		String insert = "INSERT INTO " + database2 + "." + nameDB + "copy " + "SELECT * FROM " + database1 + "."
				+ nameDB;
		System.out.println(insert);
		PreparedStatement pc = connectionDB2.prepareStatement(insert);
		pc.execute();
	}
}
