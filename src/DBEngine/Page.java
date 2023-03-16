package DBEngine;

import java.io.Serializable;

public class Page implements Serializable {
    private int _intPageNumber;
    private int _intNumberOfRows;
    private int _intMaxRows;
    private int _intNumberOfColumns;
    private String _strClusteringKeyColumn;
    private String _strTableName;
    private String[] _strColNames;
    private String[] _strColTypes;
    private Object[][] _rows;
    public Page(int intPageNumber, int intMaxRows, int intNumberOfColumns, String strClusteringKeyColumn, String strTableName, String[] strColNames, String[] strColTypes) {
        _intPageNumber = intPageNumber;
        _intMaxRows = intMaxRows;
        _intNumberOfColumns = intNumberOfColumns;
        _strClusteringKeyColumn = strClusteringKeyColumn;
        _strTableName = strTableName;
        _strColNames = strColNames;
        _strColTypes = strColTypes;
        _rows = new Object[intMaxRows][intNumberOfColumns];
        _intNumberOfRows = 0;
    }
    public int get_intPageNumber() {
        return _intPageNumber;
    }
    public void set_intPageNumber(int _intPageNumber) {
        this._intPageNumber = _intPageNumber;
    }
    public int get_intNumberOfRows() {
        return _intNumberOfRows;
    }
    public void set_intNumberOfRows(int _intNumberOfRows) {
        this._intNumberOfRows = _intNumberOfRows;
    }
    public int get_intMaxRows() {
        return _intMaxRows;
    }
    public void set_intMaxRows(int _intMaxRows) {
        this._intMaxRows = _intMaxRows;
    }
    public int get_intNumberOfColumns() {
        return _intNumberOfColumns;
    }
    public void set_intNumberOfColumns(int _intNumberOfColumns) {
        this._intNumberOfColumns = _intNumberOfColumns;
    }
    public String get_strClusteringKeyColumn() {
        return _strClusteringKeyColumn;
    }
    public void set_strClusteringKeyColumn(String _strClusteringKeyColumn) {
        this._strClusteringKeyColumn = _strClusteringKeyColumn;
    }
    public String get_strTableName() {
        return _strTableName;
    }
    public void set_strTableName(String _strTableName) {
        this._strTableName = _strTableName;
    }
    public String[] get_strColNames() {
        return _strColNames;
    }
    public void set_strColNames(String[] _strColNames) {
        this._strColNames = _strColNames;
    }
    public String[] get_strColTypes() {
        return _strColTypes;
    }
    public void set_strColTypes(String[] _strColTypes) {
        this._strColTypes = _strColTypes;
    }
    public Object[][] get_rows() {
        return _rows;
    }
    public void set_rows(Object[][] _rows) {
        this._rows = _rows;
    }
}
