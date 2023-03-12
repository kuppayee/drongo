package in.drongo.drongodb.internal.service;

import java.io.File;
import java.util.concurrent.BlockingQueue;

import in.drongo.drongodb.DrongoDBOptions;
import in.drongo.drongodb.internal.schema.FileEntry;
import in.drongo.drongodb.internal.schema.InMemoryIndex;
import in.drongo.drongodb.internal.schema.MemTable;
import in.drongo.drongodb.internal.schema.SSTable;

public class InMemoryIndexService {
    private final File directory;
    private final DrongoDBOptions drongoDBOptions;
    private InMemoryIndex inMemoryIndex;
    public InMemoryIndexService(File directory, DrongoDBOptions drongoDBOptions) {
        this.directory = directory;
        this.drongoDBOptions = drongoDBOptions;
        inMemoryIndex = new InMemoryIndex(this.directory, this.drongoDBOptions);
    }

    public void updateIndex(long now, MemTable<byte[], FileEntry> memTable) {
        inMemoryIndex.updateSSTables(now, memTable);
    }

    public void updateMetaFile(long now) {
        inMemoryIndex.updateMetaFileNode(now);
    }
    public BlockingQueue<SSTable> getSSTableIndexes() {
        return inMemoryIndex.getSSTableIndexes();
    }

}
