package in.drongo.drongodb.internal.schema;

import java.io.File;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import in.drongo.drongodb.DrongoDBOptions;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InMemoryIndex {
    private static BlockingQueue<SSTable> sstableIndexes;
    public static final String SSTABLE_FILE_NAME = "SSTABLE";
    private final MetaFile metaFile;
    private final File directory;
    private final DrongoDBOptions drongoDBOptions;
    public InMemoryIndex(File directory, DrongoDBOptions drongoDBOptions, MetaFile metaFile) {
        this.directory = directory;
        this.drongoDBOptions = drongoDBOptions;
        this.metaFile = metaFile;
        sstableIndexes = new LinkedBlockingDeque<>(4);
    }

    public BlockingQueue<SSTable> getSSTableIndexes() {
        return sstableIndexes;
    }

}
