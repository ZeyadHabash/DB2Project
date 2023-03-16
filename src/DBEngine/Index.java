package DBEngine;

public class Index {
    private String _strTableName;
    private String _strColumnName;
    private String _strClusteringKeyColumn;
    private String _strIndexName;
    private String _strTreeType;
    private int _intNumberOfNodes;
    private int _intNumberOfRows;
    public Index(String strTableName, String strColumnName, String strClusteringKeyColumn, String strIndexName, String strTreeType) {
        _strTableName = strTableName;
        _strColumnName = strColumnName;
        _strClusteringKeyColumn = strClusteringKeyColumn;
        _strIndexName = strIndexName;
        _strTreeType = strTreeType;
        _intNumberOfNodes = 0;
        _intNumberOfRows = 0;
    }
    public String get_strTableName() {
        return _strTableName;
    }
    public void set_strTableName(String _strTableName) {
        this._strTableName = _strTableName;
    }
    public String get_strColumnName() {
        return _strColumnName;
    }
    public void set_strColumnName(String _strColumnName) {
        this._strColumnName = _strColumnName;
    }
    public String get_strClusteringKeyColumn() {
        return _strClusteringKeyColumn;
    }
    public void set_strClusteringKeyColumn(String _strClusteringKeyColumn) {
        this._strClusteringKeyColumn = _strClusteringKeyColumn;
    }
    public String get_strIndexName() {
        return _strIndexName;
    }
    public void set_strIndexName(String _strIndexName) {
        this._strIndexName = _strIndexName;
    }
    public String get_strTreeType() {
        return _strTreeType;
    }
    public void set_strTreeType(String _strTreeType) {
        this._strTreeType = _strTreeType;
    }
    public int get_intNumberOfNodes() {
        return _intNumberOfNodes;
    }
    public void set_intNumberOfNodes(int _intNumberOfNodes) {
        this._intNumberOfNodes = _intNumberOfNodes;
    }
    public int get_intNumberOfRows() {
        return _intNumberOfRows;
    }
    public void set_intNumberOfRows(int _intNumberOfRows) {
        this._intNumberOfRows = _intNumberOfRows;
    }
}
