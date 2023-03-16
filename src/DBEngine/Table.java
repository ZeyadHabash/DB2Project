package DBEngine;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

import DBEngine.Page;
import DBEngine.Index;

public class Table implements Serializable {
    private String _strTableName;
    private String _strClusteringKeyColumn;
    private Hashtable<String,String> _htblColNameType;
    private Hashtable<String,String> _htblColNameMin;
    private Hashtable<String,String> _htblColNameMax;
    private Vector<Page> _pages;
    private Vector<Index> _indexes;
    private int _intNumberOfRows;
    public Table(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType, Hashtable<String,String> htblColNameMin, Hashtable<String,String> htblColNameMax) {
        _strTableName = strTableName;
        _strClusteringKeyColumn = strClusteringKeyColumn;
        _htblColNameType = htblColNameType;
        _htblColNameMin = htblColNameMin;
        _htblColNameMax = htblColNameMax;
        _pages = new Vector<Page>();
        _indexes = new Vector<Index>();
        _intNumberOfRows = 0;
    }
    public String get_strTableName() {
        return _strTableName;
    }
    public void set_strTableName(String _strTableName) {
        this._strTableName = _strTableName;
    }
    public String get_strClusteringKeyColumn() {
        return _strClusteringKeyColumn;
    }
    public void set_strClusteringKeyColumn(String _strClusteringKeyColumn) {
        this._strClusteringKeyColumn = _strClusteringKeyColumn;
    }
    public Hashtable<String, String> get_htblColNameType() {
        return _htblColNameType;
    }
    public void set_htblColNameType(Hashtable<String, String> _htblColNameType) {
        this._htblColNameType = _htblColNameType;
    }
    public Hashtable<String, String> get_htblColNameMin() {
        return _htblColNameMin;
    }
    public void set_htblColNameMin(Hashtable<String, String> _htblColNameMin) {
        this._htblColNameMin = _htblColNameMin;
    }
    public Hashtable<String, String> get_htblColNameMax() {
        return _htblColNameMax;
    }
    public void set_htblColNameMax(Hashtable<String, String> _htblColNameMax) {
        this._htblColNameMax = _htblColNameMax;
    }
    public Vector<Page> get_pages() {
        return _pages;
    }
    public void set_pages(Vector<Page> _pages) {
        this._pages = _pages;
    }
    public Vector<Index> get_indexes() {
        return _indexes;
    }
    public void set_indexes(Vector<Index> _indexes) {
        this._indexes = _indexes;
    }
    public int get_intNumberOfRows() {
        return _intNumberOfRows;
    }
    public void set_intNumberOfRows(int _intNumberOfRows) {
        this._intNumberOfRows = _intNumberOfRows;
    }
}
