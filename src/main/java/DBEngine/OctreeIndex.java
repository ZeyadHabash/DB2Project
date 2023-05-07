package DBEngine;

import DBEngine.DBApp;

public class OctreeIndex {
    private String _strTableName;
    private String _strarrColNames[];
    private Object _objarrColMinValues[];
    private Object _objarrColMaxValues[];

    public OctreeIndex(String strTableName, String[] strarrColNames, Object[] objarrColMinValues, Object[] objarrColMaxValues) {
        _strTableName = strTableName;
        _strarrColNames = strarrColNames;
        _objarrColMinValues = objarrColMinValues;
        _objarrColMaxValues = objarrColMaxValues;
    }


    public String get_strTableName() {
        return _strTableName;
    }
    public void set_strTableName(String _strTableName) {
        this._strTableName = _strTableName;
    }
    public String[] get_strarrColNames() {
        return _strarrColNames;
    }
    public void set_strarrColNames(String[] _strarrColNames) {
        this._strarrColNames = _strarrColNames;
    }
}
