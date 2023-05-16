package DBEngine.Octree;

import DBEngine.DBApp;
import Exceptions.DBAppException;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Vector;

public class Node implements Serializable {
    private Vector<OctreeEntry> _octreeEntryEntries;
    private Node[] _nodearrChildren;
    private Node _nodeParent;
    private int _intEntriesCount;
    private Object[] _objarrMinValues;
    private Object[] _objarrMaxValues;
    private Object[] _objarrMidValues;
    private String[] _strarrColTypes;

    public Node(Object[] objarrMinValues, Object[] objarrMaxValues, String[] strarrColTypes) throws DBAppException {
        _objarrMinValues = objarrMinValues;
        _objarrMaxValues = objarrMaxValues;
        _strarrColTypes = strarrColTypes;

        _octreeEntryEntries = new Vector<OctreeEntry>();
        _nodearrChildren = null; // Leaf node by default
        _intEntriesCount = 0;
        _objarrMidValues = getMidValues();


    }

    public void addEntry(Object[] objarrEntry, String strPageName, Object objEntryPk) throws DBAppException {
        // if entry already exists then add duplicate
        for (OctreeEntry entry : _octreeEntryEntries) {
            if (Arrays.equals(entry.get_objarrEntryValues(), objarrEntry)) {
                entry.addDuplicate(strPageName, objEntryPk);
                return;
            }
        }
        // Creating and adding the new entry
        OctreeEntry newEntry = new OctreeEntry(objEntryPk, objarrEntry, strPageName);
        _octreeEntryEntries.add(newEntry);
        _intEntriesCount++;
        // if node is full then create children and distribute entries
        if (_intEntriesCount > DBApp.intMaxEntriesPerNode) {
            _nodearrChildren = new Node[8]; // Create 8 children
            for (int i = 0; i < 8; i++) {
                Object objMinValues[] = setChildMin(i);
                Object objMaxValues[] = setChildMax(i);
                _nodearrChildren[i] = new Node(objMinValues, objMaxValues, _strarrColTypes);
                _nodearrChildren[i].set_nodeParent(this);

                // distribute entries to children
                for (int j = 0; j < _intEntriesCount; j++) {
                    OctreeEntry entryToDistribute = _octreeEntryEntries.get(j);
                    if (_nodearrChildren[i].entryFits(entryToDistribute.get_objarrEntryValues())) {
                        _nodearrChildren[i].addEntry(entryToDistribute);
                        removeEntry(entryToDistribute.get_objarrEntryValues());
                        j--;
                    }
                }
            }
        }
    }

    // Overloaded method to add an OctreeEntry to children
    private void addEntry(OctreeEntry entry) {
        _octreeEntryEntries.add(entry);
        _intEntriesCount++;
    }

    public void removeRow(Object[] objarrEntry, Object objEntryPk) {
        for (OctreeEntry entry : _octreeEntryEntries) {
            if (Arrays.equals(entry.get_objarrEntryValues(), objarrEntry)) {
                entry.removeDuplicate(objEntryPk);
                if (entry.isEmpty()) {
                    _octreeEntryEntries.remove(entry);
                    _intEntriesCount--;
                }
                return;
            }
        }
    }

    public void removeEntry(Object[] objarrEntry) {
        for (int i = 0; i < _intEntriesCount; i++) {
            OctreeEntry entry = _octreeEntryEntries.get(i);
            if (Arrays.equals(entry.get_objarrEntryValues(), objarrEntry)) {
                _octreeEntryEntries.remove(entry);
                _intEntriesCount--;
            }
        }
    }

    public OctreeEntry getEntry(Object[] objarrEntry) {
        for (OctreeEntry entry : _octreeEntryEntries) {
            if (Arrays.equals(entry.get_objarrEntryValues(), objarrEntry))
                return entry;
        }
        return null;
    }

//    public Vector<Node> getRangeFromChildren(Object[] objarrMin, Object[] objarrMax) {
//        Vector<Node> nodevecRange = new Vector<Node>();
//        for (Node node : _nodearrChildren) {
//            if (node.isLeaf()) {
//                if (node.rangeFits(objarrMin, objarrMax))
//                    nodevecRange.add(node);
//            } else {
//                if (node.rangeFits(objarrMin, objarrMax))
//                    nodevecRange.addAll(node.getRangeFromChildren(objarrMin, objarrMax));
//            }
//        }
//        return nodevecRange;
//    }

    public Vector<OctreeEntry> getRowsFromCondition(Object[] objarrValues, String[] strarrOperators) {
        Vector<OctreeEntry> entryvecRange = new Vector<OctreeEntry>();
        if (nodeInRange(objarrValues, strarrOperators)) {
            if (isLeaf()) {
                for (OctreeEntry entry : _octreeEntryEntries) {
                    if (entry.conditionFitsEntry(objarrValues, strarrOperators)) {
                        entryvecRange.add(entry);
                    }
                }
            } else {
                for (Node node : _nodearrChildren) {
                    Vector<OctreeEntry> tempvec = node.getRowsFromCondition(objarrValues, strarrOperators);
                    entryvecRange.addAll(tempvec);
                }
            }
        }
        return entryvecRange;
    }

    public Vector<OctreeEntry> getRowsFromCondition(Object[] objarrValues, Integer[] intarrDimensions) {
        Vector<OctreeEntry> entryvecRange = new Vector<OctreeEntry>();



        for (int i = 0; i < intarrDimensions.length; i++) {
            if (valueFits(objarrValues[i], intarrDimensions[i])) {
                if (isLeaf()) {
                    for (OctreeEntry entry : _octreeEntryEntries) {
                        if (entry.conditionFitsEntry(objarrValues, intarrDimensions)) {
                            entryvecRange.add(entry);
                        }
                    }
                } else {
                    for (Node node : _nodearrChildren) {
                        Vector<OctreeEntry> tempvec = node.getRowsFromCondition(objarrValues, intarrDimensions);
                        entryvecRange.addAll(tempvec);
                    }
                }
            }
        }
        return entryvecRange;
    }


    public void updateEntryPage(Object[] objarrEntry, Object objEntryPk, String strNewPageName) {
        OctreeEntry entry = getEntry(objarrEntry);
        entry.updatePage(objEntryPk, strNewPageName);
    }

    public Node searchChildren(Object[] objarrEntry) {
        if (isLeaf()) {
            if (entryFits(objarrEntry))
                return this;
            else
                return null;
        }
        for (Node node : _nodearrChildren) {
            if (node.entryFits(objarrEntry)) {
                if (node.isLeaf())
                    return node;
                else
                    return node.searchChildren(objarrEntry);
            }
        }
        if (entryFits(objarrEntry))
            return this;

        return null; // entry not found
    }


    public boolean isEntryInNode(Object[] objarrEntry) {
        for (OctreeEntry entry : _octreeEntryEntries) {
            if (Arrays.equals(entry.get_objarrEntryValues(), objarrEntry))
                return true;
        }
        return false;
    }

    public boolean entryFits(Object[] objarrEntry) {
        if (isRoot()) // if root then definitely fits
            return true;

        // get index of child in parent
        int currChildIndex = 0;
        for (Node child : _nodeParent.get_nodearrChildren()) {
            if (child.equals(this))
                break;
            currChildIndex++;
        }

        // check if entry fits in node
        for (int i = 0; i < _objarrMinValues.length; i++) {
            if (i == 0) { // x dimension
                if (currChildIndex % 2 == 0) { // if child is in left half
                    if (((Comparable) objarrEntry[i]).compareTo(_objarrMinValues[i]) < 0 || ((Comparable) objarrEntry[i]).compareTo(_objarrMaxValues[i]) >= 0)
                        return false;
                } else { // if child is in right half
                    if (((Comparable) objarrEntry[i]).compareTo(_objarrMinValues[i]) < 0 || ((Comparable) objarrEntry[i]).compareTo(_objarrMaxValues[i]) > 0)
                        return false;
                }
            } else if (i == 1) { // y dimension
                if (currChildIndex % 4 <= 1) { // if child is in bottom half
                    if (((Comparable) objarrEntry[i]).compareTo(_objarrMinValues[i]) < 0 || ((Comparable) objarrEntry[i]).compareTo(_objarrMaxValues[i]) >= 0)
                        return false;
                } else { // if child is in top half
                    if (((Comparable) objarrEntry[i]).compareTo(_objarrMinValues[i]) < 0 || ((Comparable) objarrEntry[i]).compareTo(_objarrMaxValues[i]) > 0)
                        return false;
                }
            } else { // z dimension
                if (currChildIndex < 4) { // if child is in front half
                    if (((Comparable) objarrEntry[i]).compareTo(_objarrMinValues[i]) < 0 || ((Comparable) objarrEntry[i]).compareTo(_objarrMaxValues[i]) >= 0)
                        return false;
                } else { // if child is in back half
                    if (((Comparable) objarrEntry[i]).compareTo(_objarrMinValues[i]) < 0 || ((Comparable) objarrEntry[i]).compareTo(_objarrMaxValues[i]) > 0)
                        return false;
                }
            }
        }
        return true;
    }

    public boolean rangeFits(Object[] objarrMin, Object[] objarrMax) {
        if (isRoot()) // if root then definitely fits
            return true;

        // get index of child in parent
        int currChildIndex = 0;
        for (Node child : _nodeParent.get_nodearrChildren()) {
            if (child.equals(this))
                break;
            currChildIndex++;
        }

        // check if range fits in node
        for (int i = 0; i < _objarrMinValues.length; i++) {
            if (i == 0) { // x dimension
                if (currChildIndex % 2 == 0) { // if child is in left half
                    if (((Comparable) objarrMin[i]).compareTo(_objarrMinValues[i]) < 0 || ((Comparable) objarrMax[i]).compareTo(_objarrMaxValues[i]) >= 0)
                        return false;
                } else { // if child is in right half
                    if (((Comparable) objarrMin[i]).compareTo(_objarrMinValues[i]) < 0 || ((Comparable) objarrMax[i]).compareTo(_objarrMaxValues[i]) > 0)
                        return false;
                }
            } else if (i == 1) { // y dimension
                if (currChildIndex % 4 <= 1) { // if child is in bottom half
                    if (((Comparable) objarrMin[i]).compareTo(_objarrMinValues[i]) < 0 || ((Comparable) objarrMax[i]).compareTo(_objarrMaxValues[i]) >= 0)
                        return false;
                } else { // if child is in top half
                    if (((Comparable) objarrMin[i]).compareTo(_objarrMinValues[i]) < 0 || ((Comparable) objarrMax[i]).compareTo(_objarrMaxValues[i]) > 0)
                        return false;
                }
            } else { // z dimension
                if (currChildIndex < 4) { // if child is in front half
                    if (((Comparable) objarrMin[i]).compareTo(_objarrMinValues[i]) < 0 || ((Comparable) objarrMax[i]).compareTo(_objarrMaxValues[i]) >= 0)
                        return false;
                } else { // if child is in back half
                    if (((Comparable) objarrMin[i]).compareTo(_objarrMinValues[i]) < 0 || ((Comparable) objarrMax[i]).compareTo(_objarrMaxValues[i]) > 0)
                        return false;
                }
            }
        }
        return true;
    }

    public boolean nodeInRange(Object[] objarrValues, String[] strArrOperators) {

        for (int i = 0; i < _objarrMinValues.length; i++) {
            if (strArrOperators[i].equals("<")) {
                if (((Comparable) objarrValues[i]).compareTo(_objarrMinValues[i]) < 0)
                    return false;
            } else if (strArrOperators[i].equals("<=")) {
                if (((Comparable) objarrValues[i]).compareTo(_objarrMinValues[i]) < 0 && !valueFits(objarrValues[i], i))
                    return false;
            } else if (strArrOperators[i].equals(">")) {
                if (((Comparable) objarrValues[i]).compareTo(_objarrMaxValues[i]) > 0)
                    return false;
            } else if (strArrOperators[i].equals(">=")) {
                if (((Comparable) objarrValues[i]).compareTo(_objarrMaxValues[i]) > 0 && !valueFits(objarrValues[i], i))
                    return false;
            } else if (strArrOperators[i].equals("=")) {
                if (!valueFits(objarrValues[i], i))
                    return false;
            } else if (strArrOperators[i].equals("!=")) {
                if (valueFits(objarrValues[i], i))
                    return false;
            }
        }
        return true;
    }

    //checks value for one dimension , used in search range for "=", "<=", ">=", "!="
    public boolean valueFits(Object objValue, int dimension) {
        if (isRoot()) {// if root then definitely fits
            if (((Comparable) objValue).compareTo(_objarrMinValues[dimension]) < 0 || ((Comparable) objValue).compareTo(_objarrMaxValues[dimension]) > 0)
                return false;
            else
                return true;
        }

        // get index of child in parent
        int currChildIndex = 0;
        for (Node child : _nodeParent.get_nodearrChildren()) {
            if (child.equals(this))
                break;
            currChildIndex++;
        }
        if (dimension == 0) { // x dimension
            if (currChildIndex % 2 == 0) { // if child is in left half
                if (((Comparable) objValue).compareTo(_objarrMinValues[dimension]) < 0 || ((Comparable) objValue).compareTo(_objarrMaxValues[dimension]) >= 0)
                    return false;
            } else { // if child is in right half
                if (((Comparable) objValue).compareTo(_objarrMinValues[dimension]) < 0 || ((Comparable) objValue).compareTo(_objarrMaxValues[dimension]) > 0)
                    return false;
            }
        } else if (dimension == 1) { // y dimension
            if (currChildIndex % 4 <= 1) { // if child is in bottom half
                if (((Comparable) objValue).compareTo(_objarrMinValues[dimension]) < 0 || ((Comparable) objValue).compareTo(_objarrMaxValues[dimension]) >= 0)
                    return false;
            } else { // if child is in top half
                if (((Comparable) objValue).compareTo(_objarrMinValues[dimension]) < 0 || ((Comparable) objValue).compareTo(_objarrMaxValues[dimension]) > 0)
                    return false;
            }
        } else { // z dimension
            if (currChildIndex < 4) { // if child is in front half
                if (((Comparable) objValue).compareTo(_objarrMinValues[dimension]) < 0 || ((Comparable) objValue).compareTo(_objarrMaxValues[dimension]) >= 0)
                    return false;
            } else { // if child is in back half
                if (((Comparable) objValue).compareTo(_objarrMinValues[dimension]) < 0 || ((Comparable) objValue).compareTo(_objarrMaxValues[dimension]) > 0)
                    return false;
            }
        }
        return true;
    }

    private Object[] setChildMin(int childNum) {
        Object[] objMinValues = new Object[_objarrMinValues.length];

        // set min x value
        if (childNum % 2 == 0)
            objMinValues[0] = _objarrMinValues[0];
        else
            objMinValues[0] = _objarrMidValues[0];

        // set min y value
        if (childNum % 4 <= 1)
            objMinValues[1] = _objarrMinValues[1];
        else
            objMinValues[1] = _objarrMidValues[1];

        // set min z value
        if (childNum < 4)
            objMinValues[2] = _objarrMinValues[2];
        else
            objMinValues[2] = _objarrMidValues[2];

        return objMinValues;
    }

    private Object[] setChildMax(int childNum) {
        Object[] objMaxValues = new Object[_objarrMaxValues.length];
        // set max x value
        if (childNum % 2 == 0)
            objMaxValues[0] = _objarrMidValues[0];
        else
            objMaxValues[0] = _objarrMaxValues[0];

        // set max y value
        if (childNum % 4 <= 1)
            objMaxValues[1] = _objarrMidValues[1];
        else
            objMaxValues[1] = _objarrMaxValues[1];

        // set max z value
        if (childNum < 4)
            objMaxValues[2] = _objarrMidValues[2];
        else
            objMaxValues[2] = _objarrMaxValues[2];

        return objMaxValues;
    }

    private Object[] getMidValues() throws DBAppException {
        Object[] objMidValues = new Object[_objarrMaxValues.length];
        for (int i = 0; i < objMidValues.length; i++) {
            if (_strarrColTypes[i].equals("java.lang.String")) {
                objMidValues[i] = getMiddleString(_objarrMaxValues[i].toString(), _objarrMinValues[i].toString());
            } else if (_strarrColTypes[i].equals("java.lang.Integer")) {
                objMidValues[i] = (((Integer) _objarrMinValues[i]) + ((Integer) _objarrMaxValues[i])) / 2;
            } else if (_strarrColTypes[i].equals("java.lang.Double")) {
                objMidValues[i] = (((Double) _objarrMinValues[i]) + ((Double) _objarrMaxValues[i])) / 2;
            } else if (_strarrColTypes[i].equals("java.util.Date")) {
                try {
                    Date min = (Date) _objarrMinValues[i];
                    Date max = (Date) _objarrMaxValues[i];
                    Date midDate = new Date((min.getTime() + max.getTime()) / 2);
                    String formattedDate = new SimpleDateFormat(DBApp.dateFormat).format(midDate);
                    objMidValues[i] = new SimpleDateFormat(DBApp.dateFormat).parse(formattedDate);
                } catch (ParseException e) {
                    throw new DBAppException("Error parsing date");
                }
            }
        }
        return objMidValues;
    }

    private String getMiddleString(String S, String T) {

        // Cast the strings to lowercase
        S = S.toLowerCase();
        T = T.toLowerCase();


        // Handle strings of different lengths
        if (S.length() > T.length()) {
            String extraLetters = S.substring(T.length());
            T = T + extraLetters;
        } else if (T.length() > S.length()) {
            String extraLetters = T.substring(S.length());
            S = S + extraLetters;
        }

        // Stores the length of the string
        int N = T.length();
        if (S.length() < T.length())
            N = S.length();

        // Stores the base 26 digits after addition
        int[] a1 = new int[N + 1];

        for (int i = 0; i < N; i++) {
            a1[i + 1] = (int) S.charAt(i) - 97
                    + (int) T.charAt(i) - 97;
        }

        // Iterate from right to left
        // and add carry to next position
        for (int i = N; i >= 1; i--) {
            a1[i - 1] += (int) a1[i] / 26;
            a1[i] %= 26;
        }

        // Reduce the number to find the middle
        // string by dividing each position by 2
        for (int i = 0; i <= N; i++) {

            // If current value is odd,
            // carry 26 to the next index value
            if ((a1[i] & 1) != 0) {

                if (i + 1 <= N) {
                    a1[i + 1] += 26;
                }
            }

            a1[i] = (int) a1[i] / 2;
        }

        String newString = "";
        for (int i = 1; i <= N; i++) {
            newString += (char) (a1[i] + 97);
        }

        return newString;
    }

    public boolean childrenEmpty() {
        for (int i = 0; i < _nodearrChildren.length; i++) {
            if (!_nodearrChildren[i].isEmpty())
                return false;
        }
        return true;
    }

    public void setNodeAsLeaf() {
        _nodearrChildren = null;
    }

    public boolean isLeaf() {
        return _nodearrChildren == null;
    }

    public boolean isRoot() {
        return _nodeParent == null;
    }

    public boolean isEmpty() {
        return _intEntriesCount == 0;
    }

    public String toString() {
        String str = "";
        if (isLeaf()) {
            str += "Leaf Node: \n";
            str += "Ranges: \n Min: " + Arrays.toString(_objarrMinValues) + "\n Max: " + Arrays.toString(_objarrMaxValues) + "\n Mid: " + Arrays.toString(_objarrMidValues) + "\n Entries: \n";
            for (int i = 0; i < _intEntriesCount; i++) {
                str += _octreeEntryEntries.get(i).toString() + " | ";
            }
        } else {
            if (isRoot())
                str += "Root Node: \n";
            else
                str += "Internal Node: \n";

            str += "Ranges: \n Min: " + Arrays.toString(_objarrMinValues) + "\n Max: " + Arrays.toString(_objarrMaxValues) + "\n Mid: " + Arrays.toString(_objarrMidValues) + "\n Entries: \n";
            for (int i = 0; i < _nodearrChildren.length; i++) {
                str += _nodearrChildren[i].toString() + " \n";
            }
        }
        return str;
    }


    // Getters and setters

    public Node[] get_nodearrChildren() {
        return _nodearrChildren;
    }

    public void set_nodearrChildren(Node[] _nodearrChildren) {
        this._nodearrChildren = _nodearrChildren;
    }

    public Node get_nodeParent() {
        return _nodeParent;
    }

    public void set_nodeParent(Node _nodeParent) {
        this._nodeParent = _nodeParent;
    }

    public int get_intEntriesCount() {
        return _intEntriesCount;
    }

    public Object[] get_objarrMinValues() {
        return _objarrMinValues;
    }

    public Object[] get_objarrMaxValues() {
        return _objarrMaxValues;
    }

    public String[] get_strarrColTypes() {
        return _strarrColTypes;
    }

    public Object[] get_objarrMidValues() {
        return _objarrMidValues;
    }

    public Vector<OctreeEntry> get_octreeEntryEntries() {
        return _octreeEntryEntries;
    }

    public void set_octreeEntryEntries(Vector<OctreeEntry> _octreeEntryEntries) {
        this._octreeEntryEntries = _octreeEntryEntries;
    }
}
