package in.drongo.drongodb.internal.schema;

import lombok.ToString;

@ToString
public class SSTable implements Comparable<SSTable> {
    public static final String VERSION = "1.0";

    private long id;
    private MemTable<byte[], FileEntry> index;

    public SSTable() {

    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public MemTable<byte[], FileEntry> getIndex() {
        return index;
    }

    public void setIndex(MemTable<byte[], FileEntry> index) {
        this.index = index;
    }

    public boolean equals(Object otherObject) {
        if (this == otherObject) {
            return true;
        }
        if (getClass() != otherObject.getClass()) {
            return false;
        }
        SSTable other = (SSTable) otherObject;
        return id == other.id;
    }

    public int hashCode() {
        return (int) id;
    }

    @Override
    public int compareTo(SSTable other) {
        return id > other.id ? -1 : 1;
    }
}
