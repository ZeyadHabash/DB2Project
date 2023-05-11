package DBEngine.Octree;

import java.util.Vector;
import java.io.Serializable;

public class OctreeEntry implements Serializable{

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

    public boolean isEmpty() {
        return _objVectorEntryPk.isEmpty();
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
