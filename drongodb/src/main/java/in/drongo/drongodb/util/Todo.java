package in.drongo.drongodb.util;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Todo {
    int i;
    int j;
    public static void main(String[] args) throws Exception {
        List<Integer> ints = List.of(4, 3, 2, 1);
        for (int i = 0; i < ints.size(); i++) {
            if (i < ints.size() - 1)
                merge(i, i + 1);
        }
    }
    static int f = 0;
    static int merge(int left, int right) {
        System.out.println("merge " + left + " into " + right);
        return right;
    }

    @Override
    public int hashCode() {
        return i;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (getClass() != other.getClass()) return false;
        Todo otherObject = (Todo) other;
        return i == otherObject.i;
    }
    
    
    
}
