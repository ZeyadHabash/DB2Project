package DBEngine;

import Exceptions.DBAppException;

import java.io.*;
import java.util.Hashtable;
import java.util.Vector;

public class Page implements Serializable {
    private String _strPageID;
    private int _intNumberOfRows;
    private Vector<Hashtable<String, Object>> _rows;
    private String _strPath;
    private String _strTableName;

    public Page(String strPageID, String strPath, String strTableName) {
        _strPageID = strPageID;
        _intNumberOfRows = 0;
        _rows = new Vector<Hashtable<String, Object>>();
        _strPath = strPath;
        _strTableName = strTableName;
        savePage();
    }

    // following method adds a row at the end of the page
    public void addRow(Hashtable<String, Object> htblNewRow) {
        _rows.add(htblNewRow);
        _intNumberOfRows++;
    }

    // overridden version of addRow that adds at a specific index instead of normal binary search
    public void addRow(Hashtable<String, Object> htblNewRow, int intRowID) {
        _rows.add(intRowID, htblNewRow);
        _intNumberOfRows++;
    }

    public void deleteRow(int intRowID) {
        _rows.remove(intRowID);
        _intNumberOfRows--;
    }

    /**
     * Updates a row in a table by replacing the values of the old row with those of the new row.
     *
     * @param intRowID   The index of the row to update.
     * @param htblNewRow A Hashtable containing the new values for the row.
     */
    public void updateRow(int intRowID, Hashtable<String, Object> htblNewRow) {
        // Retrieve the old row
        Hashtable<String, Object> htblOldRow = _rows.get(intRowID);
        Vector<String> keys = new Vector<String>();

        // Iterate through each key-value pair in htblOldRow
        htblOldRow.forEach((key, value) -> {
            // If htblNewRow contains the same key, replace the value of that key in htblOldRow with the value of that key in htblNewRow
            if (htblNewRow.containsKey(key)) {
                keys.add(key);
            }
        });

        for (int i = 0; i < keys.size(); i++) {
            htblOldRow.replace(keys.get(i), htblNewRow.get(keys.get(i)));
        }

    }


    public int getRowID(Object objClusteringKeyValue, String strClusteringKeyColumn) throws DBAppException { // returns the row with the given clustering key value
        return binarySearch(objClusteringKeyValue, strClusteringKeyColumn);
    }

    private int binarySearch(Object objClusteringKeyValue, String strClusteringKeyColumn) throws DBAppException {
        int low = 0;
        int high = _rows.size() - 1;
        int mid = (low + high) / 2;
        while (low <= high) {
            Object midClusteringKeyValue = _rows.get(mid).get(strClusteringKeyColumn);
            if (objClusteringKeyValue.equals(midClusteringKeyValue)) { // if the primary key is found return the row
                return mid;
            } else if (((Comparable) objClusteringKeyValue).compareTo(midClusteringKeyValue) < 0) { // if the primary key is less than the mid key, search in the left half
                high = mid - 1;
            } else { // if the primary key is greater than the mid key, search in the right half
                low = mid + 1;
            }
            mid = (low + high) / 2;
        }
        return -1; // if the primary key is not found return -1
    }

    public int binarySearchForInsertion(Object objClusteringKeyValue, String strClusteringKeyColumn) throws DBAppException {
        int low = 0;
        int high = _rows.size() - 1;
        int mid = (low + high) / 2;
        while (low <= high) {
            Object midClusteringKeyValue = _rows.get(mid).get(strClusteringKeyColumn);
            if (objClusteringKeyValue.equals(midClusteringKeyValue)) { // if the primary key is found throw an exception
                throw new DBAppException("Primary key is duplicated");
            } else if (((Comparable) objClusteringKeyValue).compareTo(midClusteringKeyValue) < 0) { // if the primary key is less than the mid key, search in the left half
                high = mid - 1;
            } else { // if the primary key is greater than the mid key, search in the right half
                low = mid + 1;
            }
            mid = (low + high) / 2;
        }
        return low; // return the index where the primary key should be inserted
    }

    public void savePage() {
        File file = new File(_strPath + _strTableName + _strPageID + ".class");
        try {
            FileOutputStream fos = new FileOutputStream(file);
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(this);
            oos.close();
            fos.close();
        } catch (IOException e) {
            System.out.println("Error initializing stream");
            e.printStackTrace();
        }
    }

    public static Page loadPage(String _strPath, String _strTableName, String _strPageID) throws DBAppException {
        File file = new File(_strPath + _strTableName + _strPageID + ".class");
        try {
            FileInputStream fis = new FileInputStream(file);
            ObjectInputStream ois = new ObjectInputStream(fis);
            Page page = (Page) ois.readObject();
            ois.close();
            fis.close();
            return page;
        } catch (Exception e) {
            throw new DBAppException("Page not found");
        }
    }

    public void unloadPage() {
        savePage();
        _intNumberOfRows = 0;
        _rows = null;
    }


    public void deletePage() {
        File file = new File(_strPath + _strTableName + _strPageID + ".class");
        file.delete();
    }

    @Override
    public String toString() {
        String rows = "Page ID: " + _strPageID + "\n";
        for (int i = 0; i < _rows.size(); i++) {
            rows += _rows.get(i).toString() + "\n";
        }
        return rows;
    }

    // Getters and Setters

    public String get_strPageID() {
        return _strPageID;
    }

    public void set_strPageID(String _strPageID) {
        this._strPageID = _strPageID;
    }

    public int get_intNumberOfRows() {
        return _intNumberOfRows;
    }

    public void set_intNumberOfRows(int _intNumberOfRows) {
        this._intNumberOfRows = _intNumberOfRows;
    }

    public Vector<Hashtable<String, Object>> get_rows() {
        return _rows;
    }

    public void set_rows(Vector<Hashtable<String, Object>> _rows) {
        this._rows = _rows;
    }

    public String get_strPath() {
        return _strPath;
    }

    public void set_strPath(String _strPath) {
        this._strPath = _strPath;
    }

    public String get_strTableName() {
        return _strTableName;
    }

    public void set_strTableName(String _strTableName) {
        this._strTableName = _strTableName;
    }

}
