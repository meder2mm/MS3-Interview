import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

/**
 * Open source code edited to read a CSV file into a database as well as creating a csv file for
 * data that does not match table description.
 * 
 * @author viralpatel.net and Michael Mederos
 * 
 * @version 2 - 3/22/2018
 */
public class CSVLoader {

  private static final String SQL_INSERT = "INSERT INTO ${table}(${keys}) VALUES(${values})";
  private static final String TABLE_REGEX = "\\$\\{table\\}";
  private static final String KEYS_REGEX = "\\$\\{keys\\}";
  private static final String VALUES_REGEX = "\\$\\{values\\}";

  private int lines;
  private int success;
  private int fail;
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

    String csv = "bad-data-3-22-2018.csv";
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

    // Header row of the csv file is taken first to populate key fields
    String[] headerRow = csvReader.readNext();

    if (headerRow == null) {
      throw new FileNotFoundException(
          "No columns defined in given CSV file." + "Please check the CSV file format.");
    }

    // Question mark string is created to populate query information using wildcard format
    String questionmarks = StringUtils.repeat("?,", headerRow.length);
    questionmarks = (String) questionmarks.subSequence(0, questionmarks.length() - 1);

    // Query is populated with given information from constructors and question marks
    String query = SQL_INSERT.replaceFirst(TABLE_REGEX, tableName);
    query = query.replaceFirst(KEYS_REGEX, StringUtils.join(headerRow, ","));
    query = query.replaceFirst(VALUES_REGEX, questionmarks);

    // Query is printed for error checking
    System.out.println("Query: " + query);

    String[] nextLine;
    Connection con = null;
    PreparedStatement ps = null;
    try {
      // Prepare connection to retrieve queries
      con = this.connection;
      con.setAutoCommit(false);
      ps = con.prepareStatement(query);

      if (truncateBeforeLoad) {
        // Delete data from table before loading csv
        con.createStatement().execute("DELETE FROM " + tableName);
      }

      final int batchSize = 1000;
      int count = 0;
      boolean match = true;
      while ((nextLine = csvReader.readNext()) != null) {

        if (nextLine != null) {
          // increment the received count
          lines++;
          int index = 1;

          // check and see if information is missing
          for (String string : nextLine) {
            if (string.equals("")) {
              match = false;
            }
          }
          // add query to the batch if data is correct
          if (match) {
            for (String string : nextLine) {
              ps.setString(index, string);
              index++;
            }
            ps.addBatch();
            success++;
          } else {
            // add data to badData if incorrect
            badData.add(nextLine);
            fail++;
            match = true;
          }

        }
        // perform query execution before batch size becomes to large for memory
        if (++count % batchSize == 0) {
          ps.executeBatch();
        }
      }
      ps.executeBatch(); // insert remaining records
      con.commit(); // commit all queries
      writer.writeAll(badData); // write csv file with badData
    } catch (Exception e) {
      con.rollback(); // revert to original state
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

  public void createStatistics() throws IOException {
    BufferedWriter bw = null;
    try {
      String statistics = "Number of records received: " + lines + "\n";
      statistics += "Number of records successful: " + success + "\n";
      statistics += "Number of records failed: " + fail + "\n";

      File file = new File("statistics.txt");

      if (!file.exists()) {
        file.createNewFile();
      }

      FileWriter fw = new FileWriter(file);
      bw = new BufferedWriter(fw);
      bw.write(statistics);
      System.out.println("File written Successfully");

    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
    bw.close();
  }

}

