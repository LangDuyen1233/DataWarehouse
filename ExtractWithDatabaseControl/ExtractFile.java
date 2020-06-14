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
	String jdbcURL_source, jdbcURL_dest, userName_source, userName_dest, pass_source, pass_dest, name;

//	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
//		ExtractFile ex = new ExtractFile();
//		String urlFile = "src\\week3\\thongtincanhan.txt";
//		ex.load(urlFile);
//		ex.copy("datawarehouse", "datacopy");
//
//	}

	public void load(String excelFile) throws ClassNotFoundException, SQLException, IOException {
		// connect databaseControl
		Connection connectionControl = DBConnection.getConnectionControl();
		String sqlControl = "select * from myconfig";
		PreparedStatement preSource = connectionControl.prepareStatement(sqlControl);
		ResultSet rsSource = preSource.executeQuery();
		while (rsSource.next()) {
			name = rsSource.getString("dbname");
			jdbcURL_source = rsSource.getString("source");
			userName_source = rsSource.getString("userName_source");
			pass_source = rsSource.getString("password_source");
		}
		System.out.println(name);
		System.out.println(jdbcURL_source);
		System.out.println(userName_source);
		System.out.println(pass_source);

		//
		Connection connect = DBConnection.getConnection(jdbcURL_source, userName_source, pass_source);
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
		sql = "CREATE table " + name + "(" + fields[0] + " CHAR(50)," + fields[1] + " CHAR(50)," + fields[2]
				+ " CHAR(50)," + fields[3] + " CHAR(50)," + fields[4] + " CHAR(50)," + fields[5] + " CHAR(50))";
		System.out.println(sql);
		PreparedStatement preparedStatement = connect.prepareStatement(sql);
		preparedStatement.execute();
		System.out.println("Create table Successfully :)");

		// skip header line
		String query = "INSERT INTO " + name + " VALUES(?, ?, ?, ?, ?, ?)";
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

	public void copy(String database1, String database2) throws ClassNotFoundException, SQLException {
		// ket noi toi databaseControl
		String nameDB = "";
		Connection connectionControl = DBConnection.getConnectionControl();
		String sqlControl = "select * from myconfig";
		PreparedStatement pre = connectionControl.prepareStatement(sqlControl);
		ResultSet resultSet = pre.executeQuery();
		while (resultSet.next()) {
			nameDB = resultSet.getString("dbname");
			jdbcURL_source = resultSet.getString("source");
			userName_source = resultSet.getString("userName_source");
			pass_source = resultSet.getString("password_source");
			jdbcURL_dest = resultSet.getString("dest");
			userName_dest = resultSet.getString("userName_dest");
			pass_dest = resultSet.getString("password_dest");
		}
		System.out.println(jdbcURL_source);
		System.out.println(userName_source);
		System.out.println(pass_source);
		System.out.println(jdbcURL_dest);
		System.out.println(userName_dest);
		System.out.println(pass_dest);

		// ket noi toi database chua file
		Connection connectionDB1 = DBConnection.getConnection(jdbcURL_source, userName_source, pass_source);
		System.out.println("c1 ok");
		Connection connectionDB2 = DBConnection.getConnection(jdbcURL_dest, userName_dest, pass_dest);
		System.out.println("c2 ok");

		ResultSet rs;
		Statement stmt = connectionDB1.createStatement();
		rs = stmt.executeQuery("SELECT * FROM " + nameDB);
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
