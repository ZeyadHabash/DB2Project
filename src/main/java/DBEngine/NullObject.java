package DBEngine;

public class NullObject {

    public NullObject() {
    };

    public String toString() {
        return "null";
    }

    @Override
    public boolean equals(Object obj) {
        return obj.equals(null); //compare passed attribute to null since this is a wrap class
    }
}
