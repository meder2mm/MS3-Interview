import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

/**
 * Open source code edited to read a CSV file into a database as well as creating a csv file
 * for data that does not match table description. 
 * @author viralpatel.net and Michael Mederos
 * 
 * @version 2 - 3/22/2018
 */
public class CSVLoader {

  private static final String SQL_INSERT = "INSERT INTO ${table}(${keys}) VALUES(${values})";
  private static final String TABLE_REGEX = "\\$\\{table\\}";
  private static final String KEYS_REGEX = "\\$\\{keys\\}";
  private static final String VALUES_REGEX = "\\$\\{values\\}";

  private Connection connection;
  private char seprator;

  /**
   * Public constructor to build CSVLoader object with Connection details. The connection is closed
   * on success or failure.
   * 
   * @param connection
   */
  public CSVLoader(Connection connection) {
    this.connection = connection;
    // Set default separator
    this.seprator = ',';
  }

  /**
   * Parse CSV file using OpenCSV library and load in given database table.
   * 
   * @param csvFile Input CSV file
   * @param tableName Database table name to import data
   * @param truncateBeforeLoad Truncate the table before inserting new records.
   * @throws Exception
   */
  @SuppressWarnings("resource")
  public void loadCSV(String csvFile, String tableName, boolean truncateBeforeLoad)
      throws Exception {

    String csv = "bad-data-3/22/2018.csv";
    CSVReader csvReader = null;
    CSVWriter writer = new CSVWriter(new FileWriter(csv));
    List<String[]> badData = new ArrayList<String[]>();

    if (this.connection == null) {
      throw new Exception("Not a valid connection.");
    }
    try {

      csvReader = new CSVReader(new FileReader(csvFile), this.seprator);

    } catch (Exception e) {
      e.printStackTrace();
      throw new Exception("Error occured while executing file. " + e.getMessage());
    }

    String[] headerRow = csvReader.readNext();

    if (headerRow == null) {
      throw new FileNotFoundException(
          "No columns defined in given CSV file." + "Please check the CSV file format.");
    }

    String questionmarks = StringUtils.repeat("?,", headerRow.length);
    questionmarks = (String) questionmarks.subSequence(0, questionmarks.length() - 1);

    String query = SQL_INSERT.replaceFirst(TABLE_REGEX, tableName);
    query = query.replaceFirst(KEYS_REGEX, StringUtils.join(headerRow, ","));
    query = query.replaceFirst(VALUES_REGEX, questionmarks);

    System.out.println("Query: " + query);

    String[] nextLine;
    Connection con = null;
    PreparedStatement ps = null;
    try {
      con = this.connection;
      con.setAutoCommit(false);
      ps = con.prepareStatement(query);

      if (truncateBeforeLoad) {
        // delete data from table before loading csv
        con.createStatement().execute("DELETE FROM " + tableName);
      }

      final int batchSize = 1000;
      int count = 0;
      boolean match = true;
      while ((nextLine = csvReader.readNext()) != null) {

        if (null != nextLine) {
          int index = 1;

          for (String string : nextLine) {
            if (string.equals("")) {
              match = false;
            }
          }
          if (match) {
            for (String string : nextLine) {
              ps.setString(index, string);
              index++;
            }
            ps.addBatch();
          }
          else {
            badData.add(nextLine);
          }

        }
        if (++count % batchSize == 0) {
          ps.executeBatch();
        }
      }
      ps.executeBatch(); // insert remaining records
      con.commit();
      writer.writeAll(badData);
    } catch (Exception e) {
      con.rollback();
      e.printStackTrace();
      throw new Exception(
          "Error occured while loading data from file to database." + e.getMessage());
    } finally {
      if (null != ps)
        ps.close();
      if (null != con)
        con.close();

      csvReader.close();
      writer.close();
    }
  }

  public char getSeprator() {
    return seprator;
  }

  public void setSeprator(char seprator) {
    this.seprator = seprator;
  }

}
