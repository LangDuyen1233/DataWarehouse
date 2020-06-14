package week3;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnection {
	public static Connection getConnection(String jdbcURL, String userName, String password)
			throws ClassNotFoundException, SQLException {

//		String jdbcURL = "jdbc:mysql://localhost:3306/datawarehouse";

		Connection connection = DriverManager.getConnection(jdbcURL, userName, password);
		System.out.println("success");
		return connection;
	}

	public static Connection getConnectionControl() throws SQLException {
		Connection connection = DriverManager.getConnection(
				"jdbc:mysql://localhost/databasecontrol?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC&characterEncoding=UTF-8",
				"root", "");
		System.out.println("dsd");
		return connection;
	}

}
