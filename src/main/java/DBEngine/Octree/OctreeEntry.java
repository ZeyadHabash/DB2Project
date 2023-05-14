package DBEngine.Octree;

import java.io.Serializable;
import java.util.Vector;

public class OctreeEntry implements Serializable {

    private Vector<Object> _objVectorEntryPk;
    private Object[] _objarrEntryValues;
    private Vector<String> _strVectorPages;


    public OctreeEntry(Object objEntryPk, Object[] objarrEntryValues, String strPageName) {
        _objVectorEntryPk = new Vector<Object>();
        _objarrEntryValues = objarrEntryValues;
        _strVectorPages = new Vector<String>();

        addDuplicate(strPageName, objEntryPk);
    }

    public void addDuplicate(String strPageName, Object objEntryPk) {
        _strVectorPages.add(strPageName);
        _objVectorEntryPk.add(objEntryPk);
    }

    public void removeDuplicate(Object objEntryPk) {
        int index = _objVectorEntryPk.indexOf(objEntryPk);
        _strVectorPages.remove(index);
        _objVectorEntryPk.remove(index);
    }

    public void updatePage(Object objEntryPk, String strNewPageName) {
        int index = _objVectorEntryPk.indexOf(objEntryPk);
        _strVectorPages.set(index, strNewPageName);
    }

    public boolean conditionFitsEntry(Object[] objarrValues, String[] strarrOperators) {
        for (int i = 0; i < objarrValues.length; i++) {
            if (strarrOperators[i].equals("=")) {
                if (!objarrValues[i].equals(_objarrEntryValues[i])) {
                    return false;
                }
            } else if (strarrOperators[i].equals("!=")) {
                if (objarrValues[i].equals(_objarrEntryValues[i])) {
                    return false;
                }
            } else if (strarrOperators[i].equals(">")) {
                if (((Comparable) _objarrEntryValues[i]).compareTo(objarrValues[i]) <= 0) {
                    return false;
                }
            } else if (strarrOperators[i].equals("<")) {
                if (((Comparable) _objarrEntryValues[i]).compareTo(objarrValues[i]) >= 0) {
                    return false;
                }
            } else if (strarrOperators[i].equals(">=")) {
                if (((Comparable) _objarrEntryValues[i]).compareTo(objarrValues[i]) < 0) {
                    return false;
                }
            } else if (strarrOperators[i].equals("<=")) {
                if (((Comparable) _objarrEntryValues[i]).compareTo(objarrValues[i]) > 0) {
                    return false;
                }
            }
        }
        return true;
    }

    public boolean isEmpty() {
        return _objVectorEntryPk.isEmpty();
    }

    public String toString() {
        String str = "Entry: \n";
        for (int i = 0; i < _objVectorEntryPk.size(); i++) {
            for (int j = 0; j < _objarrEntryValues.length; j++) {
                str += _objarrEntryValues[j] + " ";
            }
            str += _objVectorEntryPk.get(i) + " " + _strVectorPages.get(i) + "\n";
        }
        return str;
    }

    // Getters and Setters
    public Vector<String> get_strVectorPages() {
        return _strVectorPages;
    }

    public void set_strVectorPages(Vector<String> _strVectorPages) {
        this._strVectorPages = _strVectorPages;
    }

    public Vector<Object> get_objVectorEntryPk() {
        return _objVectorEntryPk;
    }

    public Object[] get_objarrEntryValues() {
        return _objarrEntryValues;
    }
}
