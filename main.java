import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Main method used to run the CSVLoader with given input.
 * 
 * @author Michael Mederos
 *
 */
public class main {
  private static String JDBC_CONNECTION_URL = 
      "jdbc:oracle:thin:MICHAEL/MEDEROS@localhost:1500:MS3";
  
  public static void main(String[] args) throws Exception {
    Connection connection = getCon();
    CSVLoader loader = new CSVLoader(connection);
    loader.loadCSV("ms3Interview.csv", "data", true);
  }
  
  private static Connection getCon() {
    Connection connection = null;
    try {
        Class.forName("oracle.jdbc.driver.OracleDriver");
        connection = DriverManager.getConnection(JDBC_CONNECTION_URL);

    } catch (ClassNotFoundException e) {
        e.printStackTrace();
    } catch (SQLException e) {
        e.printStackTrace();
    }

    return connection;
}
}
