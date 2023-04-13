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

    public static void main(String[] args) throws DBAppException, IOException {
        DBApp dbApp = new DBApp();
        dbApp.init();
        String strTableName = "Student";
        Hashtable<String, String> htblColNameType = new Hashtable<>();
        Hashtable<String, String> htblColNameMin = new Hashtable<String, String>();
        Hashtable<String, String> htblColNameMax = new Hashtable<String, String>();

        htblColNameType.put("id", "java.lang.Integer");
        htblColNameMin.put("id", "0");
        htblColNameMax.put("id", "1000000000");

        htblColNameType.put("name", "java.lang.String");
        htblColNameMin.put("name", "A");
        htblColNameMax.put("name", "ZZZZZZZZZZZZZZZZZZZZZZZZZ");

        htblColNameType.put("gpa", "java.lang.Double");
        htblColNameMin.put("gpa", "0.0");
        htblColNameMax.put("gpa", "4.0");

        dbApp.createTable(strTableName, "id", htblColNameType, htblColNameMin, htblColNameMax);


//        Hashtable htblColNameValue = new Hashtable();
//        htblColNameValue.put("id", new Integer(2343432));
//        htblColNameValue.put("name", new String("Ahmed Noor"));
//        htblColNameValue.put("gpa", new Double(0.95));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new Integer(453455));
//        htblColNameValue.put("name", new String("Ahmed Noor"));
//        htblColNameValue.put("gpa", new Double(0.95));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new Integer(5674567));
//        htblColNameValue.put("name", new String("Dalia Noor"));
//        htblColNameValue.put("gpa", new Double(1.25));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);

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
            tables.forEach(table -> System.out.println(table.get_strTableName())); // TODO: remove this
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

        // verify that table name is unique
        for (Table table : tables) {
            if (table.get_strTableName().equals(strTableName))
                throw new DBAppException("Table name already exists");
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
        CSVWriter writer = new CSVWriter(new FileWriter(metadataFile, true),CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.DEFAULT_ESCAPE_CHARACTER,CSVWriter.DEFAULT_LINE_END); // open csv file
        entrySet = htblColNameType.entrySet(); // what is ht???
        for (Entry<String, String> entry : entrySet) {
            String columnName = entry.getKey(); // get column name
            String columnType = entry.getValue(); // get column type
            boolean clusteringKey = columnName.equals(strClusteringKeyColumn); // check if column is clustering key
            String min = htblColNameMin.get(columnName); // get min value
            String max = htblColNameMax.get(columnName); // get max value
            String[] csvEntry = {strTableName, columnName, columnType, Boolean.toString(clusteringKey), "null", "null", min, max}; // create csv entry
            writer.writeNext(csvEntry); // write csv entry to csv file
        }
        writer.close(); // close csv file
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
        verifyRow(tableToInsertInto, htblColNameValue);

        binarySearchAndInsert(tableToInsertInto, htblColNameValue); // insert the record

        tableToInsertInto.unloadTable(); // unload the table
    }

    // following method updates one row only
    // htblColNameValue holds the key and new value
    // htblColNameValue will not include clustering key as column name
    // strClusteringKeyValue is the value to look for to find the row to update.
    public void updateTable(String strTableName,
                            String strClusteringKeyValue,
                            Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException {
        // Check if the table exists
        // If it doesn't, throw an exception
        // If it does, update the record
        // Create a new record object
        // Add the record to the table
        // Save the table

        Table tableToUpdate = getTableFromName(strTableName); // get reference to table
        tableToUpdate.loadTable(); // load the table into memory

        // verify that the input row violates no constraints
        verifyRow(tableToUpdate, htblColNameValue);

        // get the index of the row to update
        int index = binarySearch(tableToUpdate, strClusteringKeyValue);

        // update the row
        tableToUpdate.updateRow(index, htblColNameValue);

        // TODO: sort table after updating (?)
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

        //exceptions when deleting
        /*
        1- no such row
        ???
         */

        Table tableToDeleteFrom = getTableFromName(strTableName); // get reference to table
        tableToDeleteFrom.loadTable(); // load the table into memory

        // find indexes of rows to delete
        // how to search? binary search? but it's unsorted??? idk tbh :(
        // store all indexes to delete in an array the delete them? idk aswell
        // or just delete them as we find them? idk

        if (htblColNameValue.containsKey(tableToDeleteFrom.get_strClusteringKeyColumn())) {
            // if the clustering key is in the hashtable, delete only one row
            String clusteringKeyValue = (String) htblColNameValue.get(tableToDeleteFrom.get_strClusteringKeyColumn());
            int index = binarySearch(tableToDeleteFrom, clusteringKeyValue); // find the index of the row to delete using binary search
            tableToDeleteFrom.deleteRow(index); // delete the row at that index
        } else {
            // if the clustering key is not in the hashtable, delete all rows that match the other conditions
            // currently it goes through the rows linearly
            // maybe try to switch it to use binary search? idk how though
            for (int i = 0; i < tableToDeleteFrom.get_intNumberOfRows(); i++) { // loop through all rows in the table
                boolean toDelete = true; // assume the row should be deleted
                for (Entry<String, Object> entry : htblColNameValue.entrySet()) { // loop through all entries in the hashtable
                    if (!tableToDeleteFrom.getRowFromIndex(i).get(entry.getKey()).equals(entry.getValue())) { // check if the row value matches the hashtable value for each column name
                        toDelete = false; // if not, set toDelete to false and break the loop
                        break;
                    }
                }
                if (toDelete) { // if toDelete is still true after checking all columns
                    tableToDeleteFrom.deleteRow(i); // delete the row
                    i--; // decrement i to account for the deleted row
                }
            }
        }
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

    private void verifyRow(Table table, Hashtable<String, Object> htblRow) throws DBAppException, IOException {
        // verify that the input row violates no constraints
        Set<Entry<String, Object>> entrySet = htblRow.entrySet();
        BufferedReader br = new BufferedReader(new FileReader(metadataFile)); // read csv file
        for (Entry<String, Object> entry : entrySet) {
            String columnName = entry.getKey();
            Object columnValue = entry.getValue();
            String columnType = table.get_htblColNameType().get(columnName);

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
                if (values[0].equals(table.get_strTableName()) && values[1].equals(columnName)) { // check if table name and column name match
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
                int min = Integer.parseInt(table.get_htblColNameMin().get(columnName));
                int max = Integer.parseInt(table.get_htblColNameMax().get(columnName));
                if (value < min || value > max) {
                    throw new DBAppException("Value out of range");
                }
            } else if (columnValue instanceof Double) {
                double value = (double) columnValue;
                double min = Double.parseDouble(table.get_htblColNameMin().get(columnName));
                double max = Double.parseDouble(table.get_htblColNameMax().get(columnName));
                if (value < min || value > max) {
                    throw new DBAppException("Value out of range");
                }
            } else if (columnValue instanceof String) {
                String value = (String) columnValue;
                String min = table.get_htblColNameMin().get(columnName);
                String max = table.get_htblColNameMax().get(columnName);
                if (value.compareTo(min) < 0 || value.compareTo(max) > 0) {
                    throw new DBAppException("Value out of range");
                }
            } else if (columnValue instanceof Date) {
                Date value = (Date) columnValue;
                Date min = new Date(table.get_htblColNameMin().get(columnName));
                Date max = new Date(table.get_htblColNameMax().get(columnName));
                if (value.compareTo(min) < 0 || value.compareTo(max) > 0) {
                    throw new DBAppException("Value out of range");
                }
            }
        }
        br.close();
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


    // This method performs a binary search on a table object
    private int binarySearch(Table tableToSearchIn, String strClusteringKeyValue) {
        // Call the recursive helper method
        return binarySearchHelper(tableToSearchIn, 0, tableToSearchIn.get_intNumberOfRows() - 1, strClusteringKeyValue);
    }
    private int binarySearchHelper(Table tableToSearchIn, int intMinIndex, int intMaxIndex, String strClusteringKeyValue) {
        // Calculate the middle index of the table
        int mid = (intMinIndex + intMaxIndex) / 2;
        // Get the clustering key value of the row at the middle index
        Object midClusteringKey = tableToSearchIn.getClusteringKeyFromRow(tableToSearchIn.getRowFromIndex(mid));
        // Check if the minimum index is greater than the maximum index, or if the middle index is out of bounds
        if (intMinIndex > intMaxIndex || mid < 0 || mid >= tableToSearchIn.get_intNumberOfRows())
            return -1; // Return -1 to indicate that the key value was not found

        // Check if the clustering key value at the middle index matches the target value
        if (midClusteringKey.equals(strClusteringKeyValue))
            return mid; // Return the middle index as the result
            // Check if the clustering key value at the middle index is greater than the target value
        else if (((Comparable) midClusteringKey).compareTo(strClusteringKeyValue) > 0)
            return binarySearchHelper(tableToSearchIn, intMinIndex, mid - 1, strClusteringKeyValue); // Recursively search in the left half of the table
        else
            return binarySearchHelper(tableToSearchIn, mid + 1, intMaxIndex, strClusteringKeyValue); // Recursively search in the right half of the table
    }
}
