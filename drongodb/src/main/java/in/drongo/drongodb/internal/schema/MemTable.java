package in.drongo.drongodb.internal.schema;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import in.drongo.drongodb.DrongoDBOptions;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MemTable<K, V> implements Serializable {
    private static final long serialVersionUID = 1L;
    private final Map<ByteBuffer, FileEntry> balancedBST = new TreeMap<>();
    private long memTableThresholdSize;
    private final DrongoDBOptions drongoDBOptions;

    public MemTable(DrongoDBOptions drongoDBOptions) {
        this.drongoDBOptions = drongoDBOptions;
        memTableThresholdSize = this.drongoDBOptions.ssTableThresholdSize;
    }

    public boolean put(byte[] key, FileEntry value) {
        log.debug("FileEntry size: " + value.array().length);
        log.debug("memTableThresholdSize: " + memTableThresholdSize);
        if (value.array().length <= memTableThresholdSize) {
            balancedBST.put(ByteBuffer.wrap(key), value);
            memTableThresholdSize -= value.array().length;
            log.debug("remaining memTableThresholdSize: " + memTableThresholdSize);
            return true;
        }
        return false;
    }

    public void setMemTableThresholdSize(final long memTableThresholdSize) {
        this.memTableThresholdSize = memTableThresholdSize;
    }

    public int remainingSize() {
        return (int) memTableThresholdSize;
    }

    public boolean hasRemainingSize() {
        return memTableThresholdSize > 0;
    }

    public int size() {
        return (int) (drongoDBOptions.ssTableThresholdSize - memTableThresholdSize);
    }

    public Map<ByteBuffer, FileEntry> getFileEntries() {
        return balancedBST;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        int[] j = { 0 };
        balancedBST.forEach((k, v) -> {
            if (j[0]++ != 0) {
                sb.append(", ");
            }
            sb.append(new String(k.array()));
        });
        return sb.toString();
    }

}
