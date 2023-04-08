package DBEngine;

import Exceptions.DBAppException;
import com.opencsv.CSVWriter;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

public class DBApp {

    public static int intMaxRows;
    private Vector<Table> tables;
    private File metadataFile;

    public static void main(String[] args) {
    }

    public void init() {
        // create data folder if it doesn't exist
        File dataFolder = new File("data");
        if (!dataFolder.exists()) {
            dataFolder.mkdir();
        }
        // go to data folder and create metadata.csv if it doesn't exist
        metadataFile = new File("data/metadata.csv");
        if (!metadataFile.exists()) {
            try {
                metadataFile.createNewFile();
            } catch (Exception e) {
                System.out.println("Error creating metadata file");
            }
        }
        // TODO: store min and max values in config file
        // get max rows from config file
        Properties prop = new Properties();
        try {
            prop.load(new FileInputStream("src/main/resources/DBApp.config"));
            intMaxRows = Integer.parseInt(prop.getProperty("MaximumRowsCountinTablePage"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Read metadata.csv and add tables to tables vector
        tables = new Vector<Table>();
        try {
            BufferedReader br = new BufferedReader(new FileReader(metadataFile));
            String line = br.readLine();
            while (line != null) {
                String[] lineData = line.split(",");
                String tableName = lineData[0];
                // check if table already exists
                boolean tableExists = false;
                for (Table table : tables) {
                    if (table.get_strTableName().equals(tableName)) {
                        tableExists = true;
                        break;
                    }
                }
                if (!tableExists) {
                    Table newTable = new Table(tableName, "data/");
                    tables.add(newTable);
                }
                line = br.readLine();
            }
            br.close();
        } catch (Exception e) {
            System.out.println("Error reading metadata file");
        }

        // get max octree nodes from config file

        // do the rest of the initialization (Still need to figure out what that is)
    }

    // following method creates one table only
    // strClusteringKeyColumn is the name of the column that will be the primary
    // key and the clustering column as well. The data type of that column will
    // be passed in htblColNameType
    // htblColNameValue will have the column name as key and the data
    // type as value
    // htblColNameMin and htblColNameMax for passing minimum and maximum values
    // for data in the column. Key is the name of the column
    //
    //create min and max hashtables in main before create table
    public void createTable(String strTableName,
                            String strClusteringKeyColumn,
                            Hashtable<String, String> htblColNameType,
                            Hashtable<String, String> htblColNameMin,
                            Hashtable<String, String> htblColNameMax) throws DBAppException, IOException {

        //min/max values based on what?
        //add constraint to config file?

        // verify that none of the inputs are null
        if (strTableName == null) {
            throw new DBAppException("Table name is null");
        }
        if (htblColNameType.get(strClusteringKeyColumn) == null) {
            throw new DBAppException("Clustering key column not found");
        }
        if (htblColNameMin.get(strClusteringKeyColumn) == null) {
            throw new DBAppException("Clustering key column min value not found");
        }
        if (htblColNameMax.get(strClusteringKeyColumn) == null) {
            throw new DBAppException("Clustering key column max value not found");
        }

        // verify datatype of all hashtables
        Set<Entry<String, String>> entrySet = htblColNameType.entrySet();
        for (Entry<String, String> entry : entrySet) {
            String columnName = entry.getKey();
            String columnType = entry.getValue();
            if (!(columnType.equals("java.lang.Integer") || columnType.equals("java.lang.Double") || columnType.equals("java.lang.String") || columnType.equals("java.util.Date"))) {
                throw new DBAppException("Invalid data type");
            } else {
                if (htblColNameMin.get(columnName) == null) {
                    throw new DBAppException("Column min value not found");
                }
                if (htblColNameMax.get(columnName) == null) {
                    throw new DBAppException("Column max value not found");
                }
            }
        }

        // check if table already exists
        BufferedReader br = new BufferedReader(new FileReader(metadataFile)); // read csv file
        String line = br.readLine();
        while (line != null) { // loop over all lines
            String[] values = line.split(",");
            if (values[0].equals(strTableName)) { // check if table name already exists
                throw new DBAppException("Table already exists"); // if it does, throw exception
            }
            line = br.readLine();
        }
        br.close();

        // write to metadata file
        CSVWriter writer = new CSVWriter(new FileWriter(metadataFile, true));
        //String csvEntry = strTableName;
        entrySet = htblColNameType.entrySet(); // what is ht???
        for (Entry<String, String> entry : entrySet) {
            //csvEntry = strTableName;
            String columnName = entry.getKey();
            String columnType = entry.getValue();
            boolean clusteringKey = columnName.equals(strClusteringKeyColumn);
            //csvEntry += ", " + columnName + "," + columnType + "," + clusteringKey + ",null,null,";
            String min = htblColNameMin.get(columnName);
            String max = htblColNameMax.get(columnName);
            //csvEntry += min + "," + max;
            String[] csvEntry = {strTableName, columnName, columnType, Boolean.toString(clusteringKey), "null", "null", min, max}; // create csv entry
        }

        Table table = new Table(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin,
                htblColNameMax, "data/"); // not sure about the path

        tables.add(table); // add table to tables vector
        table.unloadTable(); // unload table from memory
    }

    // following method creates an octree
    // depending on the count of column names passed.
    // If three column names are passed, create an octree.
    // If only one or two column names is passed, throw an Exception.
    public void createIndex(String strTableName, String strarrColName) throws DBAppException {
        // Check if the table exists
        // If it doesn't, throw an exception
        // If it does, create a new index
        // Create a new index object
        // Add the index to the table
        // Save the table
    }

    // following method inserts one row only.
    // htblColNameValue must include a value for the primary key
    public void insertIntoTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException {
        // Check if the table exists
        // If it doesn't, throw an exception
        // If it does, insert the record
        // Create a new record object
        // Add the record to the table
        // Save the table

        //exceptions when inserting
        /*
        1-data type mismatch when comparing to csv
        2-primary key duplicated
         */

        Table tableToInsertInto = getTableFromName(strTableName); // get reference to table
        tableToInsertInto.loadTable(); // load the table into memory

        // verify that the input row violates no constraints
        Set<Entry<String, Object>> entrySet = htblColNameValue.entrySet();
        BufferedReader br = new BufferedReader(new FileReader(metadataFile)); // read csv file
        for (Entry<String, Object> entry : entrySet) {
            String columnName = entry.getKey();
            Object columnValue = entry.getValue();
            String columnType = tableToInsertInto.get_htblColNameType().get(columnName);

            // check if data type matches within the row
            if (columnType.equals("java.lang.Integer")) {
                if (!(columnValue instanceof Integer)) {
                    throw new DBAppException("Data type mismatch");
                }
            } else if (columnType.equals("java.lang.Double")) {
                if (!(columnValue instanceof Double)) {
                    throw new DBAppException("Data type mismatch");
                }
            } else if (columnType.equals("java.lang.String")) {
                if (!(columnValue instanceof String)) {
                    throw new DBAppException("Data type mismatch");
                }
            } else if (columnType.equals("java.util.Date")) {
                if (!(columnValue instanceof Date)) {
                    throw new DBAppException("Data type mismatch");
                }
            }

            // check if data types match the table's data types
            String line = br.readLine();
            while (line != null) { // loop over all lines
                String[] values = line.split(","); // split line into values
                if (values[0].equals(strTableName) && values[1].equals(columnName)) { // check if table name and column name match
                    if (!values[2].equals(columnType)) { // check if data types match
                        throw new DBAppException("Data type mismatch"); // if they don't, throw exception
                    }
                    break; // if column name and table name match, break out of loop
                }
                line = br.readLine(); // read next line if column name and table name don't match
            }

            // check if all columns are within min and max
            if (columnValue instanceof Integer) {
                int value = (int) columnValue;
                int min = Integer.parseInt(tableToInsertInto.get_htblColNameMin().get(columnName));
                int max = Integer.parseInt(tableToInsertInto.get_htblColNameMax().get(columnName));
                if (value < min || value > max) {
                    throw new DBAppException("Value out of range");
                }
            } else if (columnValue instanceof Double) {
                double value = (double) columnValue;
                double min = Double.parseDouble(tableToInsertInto.get_htblColNameMin().get(columnName));
                double max = Double.parseDouble(tableToInsertInto.get_htblColNameMax().get(columnName));
                if (value < min || value > max) {
                    throw new DBAppException("Value out of range");
                }
            } else if (columnValue instanceof String) {
                String value = (String) columnValue;
                String min = tableToInsertInto.get_htblColNameMin().get(columnName);
                String max = tableToInsertInto.get_htblColNameMax().get(columnName);
                if (value.compareTo(min) < 0 || value.compareTo(max) > 0) {
                    throw new DBAppException("Value out of range");
                }
            } else if (columnValue instanceof Date) {
                Date value = (Date) columnValue;
                Date min = new Date(tableToInsertInto.get_htblColNameMin().get(columnName));
                Date max = new Date(tableToInsertInto.get_htblColNameMax().get(columnName));
                if (value.compareTo(min) < 0 || value.compareTo(max) > 0) {
                    throw new DBAppException("Value out of range");
                }
            }
        }
        br.close();

        binarySearchAndInsert(tableToInsertInto, htblColNameValue); // insert the record

        tableToInsertInto.unloadTable(); // unload the table
    }

    // following method updates one row only
    // htblColNameValue holds the key and new value
    // htblColNameValue will not include clustering key as column name
    // strClusteringKeyValue is the value to look for to find the row to update.
    public void updateTable(String strTableName,
                            String strClusteringKeyValue,
                            Hashtable<String, Object> htblColNameValue) throws DBAppException {
        // Check if the table exists
        // If it doesn't, throw an exception
        // If it does, update the record
        // Create a new record object
        // Add the record to the table
        // Save the table
    }

    // following method could be used to delete one or more rows.
    // htblColNameValue holds the key and value. This will be used in search
    // to identify which rows/tuples to delete.
    // htblColNameValue entries are ANDed together
    public void deleteFromTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue) throws DBAppException {
        // Check if the table exists
        // If it doesn't, throw an exception
        // If it does, delete the record
        // Create a new record object
        // Add the record to the table
        // Save the table
    }

    public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
                                    String[] strarrOperators) throws DBAppException {
        // Check if the table exists
        // If it doesn't, throw an exception
        // If it does, select the records
        // Create a new record object
        // Add the record to the table
        // Save the table
        // Return the iterator
        return null;
    }

    // Helper methods

    private Table getTableFromName(String strTableName) throws DBAppException {
        // Check if the table exists
        // If it doesn't, throw an exception
        // If it does, return the table
        for (Table table : tables) {
            if (table.get_strTableName().equals(strTableName)) {
                return table;
            }
        }
        throw new DBAppException("Table not found");
    }


    private void binarySearchAndInsert(Table tableToInsertTo, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        int left = 0; // Initialize left index to 0
        int right = tableToInsertTo.get_intNumberOfRows() - 1; // Initialize right index to last index of the array
        int mid;
        Object midClusteringKey;
        Object newRowClusteringKey = tableToInsertTo.getClusteringKeyFromRow(htblColNameValue);

        // Perform binary search to check if element exists
        while (left <= right) { // Loop until left index becomes greater than right index
            mid = (left + right) / 2; // Find the middle index
            midClusteringKey = tableToInsertTo.getClusteringKeyFromRow(tableToInsertTo.getRowFromIndex(mid));
            if (midClusteringKey.equals(newRowClusteringKey)) { // If the middle element is equal to the given element
                throw new DBAppException("Primary key duplicated"); // Throw an exception
            } else if (((Comparable) midClusteringKey).compareTo(newRowClusteringKey) < 0) { // If the middle element is less than the given element
                left = mid + 1; // Update left index to mid+1
            } else { // If the middle element is greater than the given element
                right = mid - 1; // Update right index to mid-1
            }
        }
        // Element doesn't exist in the array, insert it at the correct position
        tableToInsertTo.insertRow(htblColNameValue, left);
    }

    private int binarySearch(Table tableToSearchIn, int intMinIndex, int intMaxIndex, Hashtable<String, Object> htblColNameValue) {
        int mid = (intMinIndex + intMaxIndex) / 2;
        Object midClusteringKey = tableToSearchIn.getClusteringKeyFromRow(tableToSearchIn.getRowFromIndex(mid));
        Object rowClusteringKey = tableToSearchIn.getClusteringKeyFromRow(htblColNameValue);
        if (intMinIndex > intMaxIndex || mid < 0 || mid >= tableToSearchIn.get_intNumberOfRows())
            return -1;
        if (midClusteringKey.equals(rowClusteringKey))
            return mid;
        else if (((Comparable) midClusteringKey).compareTo(rowClusteringKey) > 0)
            return binarySearch(tableToSearchIn, intMinIndex, mid - 1, htblColNameValue);
        else
            return binarySearch(tableToSearchIn, mid + 1, intMaxIndex, htblColNameValue);
    }
}
