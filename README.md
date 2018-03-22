# MS3-Interview

# DESCRIPTION

1. We need a Java application that will consume a CSV file, parse the data and insert to a SQLlite In-Memory database.  

  - Table X has 10 columns A, B, C, D, E, F, G, H, I, J which correspond with the CSV file column header names.

  - Include all DDL in submitted repository

  - Create your own MySQL DB


2. The data sets can be extremely large so be sure the processing is optimized with efficiency in mind.  


3. Each record needs to be verified to contain the right number of data elements to match the columns.  

  - Records that do not match the column count must be written to the bad-data-<timestamp>.csv file

  - Elements with commas will be double quoted


4. At the end of the process write statistics to a log file

  - num of records received

  - num of records successful

  - num of records failed

# Disclaimer

Some items may have to be formatted to fit the path pointed to by java code in order to get a proper result. MySQL was downloaded onto my computer with corrupted data and as such I was unable to set it up properly within the given time to create a database. Although I did use SQLite to create a database which is included in the repository. All files needed for the project have been included.
