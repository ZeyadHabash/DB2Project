import java.io.File;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;

import Exceptions.DBAppException;
import DBEngine.SQLTerm;

public class DBApp {
    public static void main(String[] args) {
        
    }

    public void init() {
        File f = new File("data");
        if (!f.exists()) {
            f.mkdir();
        }
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
    public void createTable(String strTableName,
                            String strClusteringKeyColumn,
                            Hashtable<String,String> htblColNameType,
                            Hashtable<String,String> htblColNameMin,
                            Hashtable<String,String> htblColNameMax ) throws DBAppException{
        // Check if the table already exists
        // If it does, throw an exception
        // If it doesn't, create a new table
        // Create a new table object
        // Create a new table directory
        // Create a new metadata file
        // Create a new pages directory
        // Create a new page file
        // Create a new page object
        // Add the page to the table
        // Add the table to the database
        // Save the table
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
                                Hashtable<String,Object> htblColNameValue) throws DBAppException {
        // Check if the table exists
        // If it doesn't, throw an exception
        // If it does, insert the record
        // Create a new record object
        // Add the record to the table
        // Save the table
    }
    
    // following method updates one row only
    // htblColNameValue holds the key and new value
    // htblColNameValue will not include clustering key as column name
    // strClusteringKeyValue is the value to look for to find the row to update.
    public void updateTable(String strTableName,
                            String strClusteringKeyValue,
                            Hashtable<String,Object> htblColNameValue) throws DBAppException {
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
    // htblColNameValue enteries are ANDED together
    public void deleteFromTable(String strTableName,
                                Hashtable<String,Object> htblColNameValue) throws DBAppException {
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




    // helper functions

    private void saveTable(Table table) {
        // Save the table
    }

    private Table loadTable(String strTableName) {
        // Load the table
        return null;
    }

    private void savePage(Page page) {
        // Save the page
    }

    private Page loadPage(String strTableName, int pageNumber) {
        // Load the page
        return null;
    }

    private void saveRecord(Record record) {
        // Save the record
    }

    private Record loadRecord(String strTableName, int pageNumber, int recordNumber) {
        // Load the record
        return null;
    }

    private createMetaDataFile(String strTableName) {
        // Create a new metadata file
    }

    private createTableDirectory(String strTableName) {
        // Create a new table directory
    }

    private createPagesDirectory(String strTableName) {
        // Create a new pages directory
    }

    private createPageFile(String strTableName, int pageNumber) {
        // Create a new page file
    }

    private createIndexFile(String strTableName, String strIndexName) {
        // Create a new index file
    }

    private createIndexDirectory(String strTableName, String strIndexName) {
        // Create a new index directory
    }
}
