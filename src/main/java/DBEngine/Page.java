package DBEngine;

import java.io.*;
import java.util.Hashtable;
import java.util.Vector;

public class Page implements Serializable {
    private transient int _intPageID;
    private int _intNumberOfRows;
    private Vector<Hashtable<String, Object>> _rows;
    private transient String _strPath;
    private String _strTableName;

    public Page(int intPageID, String strPath, String strTableName) {
        _intPageID = intPageID;
        _intNumberOfRows = 0;
        _rows = new Vector<Hashtable<String, Object>>();
        _strPath = strPath;
        _strTableName = strTableName;
        savePage();
    }

    public void addRow(Hashtable<String, Object> htblNewRow) {
        _rows.add(htblNewRow);
        _intNumberOfRows++;
        savePage();
    }

    // overridden version of addRow that adds at a specific index instead of at end of page
    public void addRow(Hashtable<String, Object> htblNewRow, int intRowIndex) {
        _rows.add(intRowIndex, htblNewRow);
        _intNumberOfRows++;
        savePage();
    }

    public void deleteRow(int intRowIndex) {
        _rows.remove(intRowIndex);
        _intNumberOfRows--;
        savePage();
    }

    /**
     * Updates a row in a table by replacing the values of the old row with those of the new row.
     *
     * @param intRowIndex The index of the row to update.
     * @param htblNewRow  A Hashtable containing the new values for the row.
     */
    public void updateRow(int intRowIndex, Hashtable<String, Object> htblNewRow) {
        // Retrieve the old row
        Hashtable<String, Object> htblOldRow = _rows.get(intRowIndex);
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

        // Save the page
        savePage();
    }

    public void savePage() {
        //deletePage();
        File file = new File(_strPath + _strTableName + _intPageID + ".class");
        try {
            FileOutputStream fos = new FileOutputStream(file);
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(this);
            oos.close();
            fos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // not sure if correct
    public void loadPage() {
        File file = new File(_strPath + _strTableName + _intPageID + ".class");
        try {
            FileInputStream fis = new FileInputStream(file);
            ObjectInputStream ois = new ObjectInputStream(fis);
            Page page = (Page) ois.readObject();
            ois.close();
            fis.close();
            _intPageID = page.get_intPageID();
            _intNumberOfRows = page.get_intNumberOfRows();
            _rows = page.get_rows();
            _strPath = page.get_strPath();
            _strTableName = page.get_strTableName();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void unloadPage() {
        _intNumberOfRows = 0;
        _rows = null;
    }


    public void deletePage() {
        File file = new File(_strPath + _strTableName + _intPageID + ".class");
        file.delete();
    }

    @Override
    public String toString() {
        String rows = "Page ID: " + _intPageID + "\n";
        for (int i = 0; i < _rows.size(); i++) {
            rows += _rows.get(i).toString() + "\n";
        }
        return rows;
    }

    // Getters and Setters

    public int get_intPageID() {
        return _intPageID;
    }

    public void set_intPageID(int _intPageID) {
        this._intPageID = _intPageID;
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
