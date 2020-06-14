package week3;

import java.io.IOException;
import java.sql.SQLException;

public class Test {
	public static void main(String[] args) throws Exception, SQLException, IOException {
		String file = "src\\week3\\thongtincanhan.txt";
		ExtractFile extractFile = new ExtractFile();
		extractFile.load(file);
		extractFile.copy("datawarehouse", "datacopy");
	}

}
