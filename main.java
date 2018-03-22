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

  public static void main(String[] args) throws Exception {
    Connection connection = DriverManager.getConnection("jdbc:sqlite:C:/MS3.db");
    CSVLoader loader = new CSVLoader(connection);
    loader.loadCSV("C:\\ms3Interview.csv", "data", true);
    loader.createStatistics();
  }

}
