package DBEngine;

import Exceptions.DBAppException;

public class SQLTerm {
    public String _strTableName;
    public String _strColumnName;
    public String _strOperator;
    public Object _objValue;
    public SQLTerm() {
    }
    public SQLTerm(String strTableName, String strColumnName, String strOperator, Object objValue) {
        _strTableName = strTableName;
        _strColumnName = strColumnName;
        _strOperator = strOperator;
        _objValue = objValue;
    }

    public String toString(){
        return _strTableName + " " + _strColumnName + " " + _strOperator + " " + _objValue;
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
    public String get_strOperator() {
        return _strOperator;
    }
    public void set_strOperator(String _strOperator) throws DBAppException {
        if (_strOperator.equals("=") || _strOperator.equals("!=") || _strOperator.equals(">") || _strOperator.equals("<") || _strOperator.equals(">=") || _strOperator.equals("<="))
            this._strOperator = _strOperator;
        else
            throw new DBAppException("Invalid operator");
    }
    public Object get_objValue() {
        return _objValue;
    }
    public void set_objValue(Object _objValue) {
        this._objValue = _objValue;
    }
}
