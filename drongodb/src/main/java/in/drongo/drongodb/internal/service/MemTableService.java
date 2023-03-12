package in.drongo.drongodb.internal.service;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import in.drongo.drongodb.DrongoDBOptions;
import in.drongo.drongodb.exception.DrongoDBFileEntryException;
import in.drongo.drongodb.internal.schema.FileEntry;
import in.drongo.drongodb.internal.schema.MemTable;
import in.drongo.drongodb.internal.schema.SSTable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MemTableService {
    public static final int MAX_RETRY_NUMBER = 4;
    private final File directory;
    private final DrongoDBOptions drongoDBOptions;
    private final CrashRecoveryService crashRecoveryService;
    private SSTableService ssTableService;
    private volatile MemTable<byte[], FileEntry> memTable;
    private InMemoryIndexService inMemoryIndexService;
    private ReentrantReadWriteLock balancedBSTLock = new ReentrantReadWriteLock();
    private final Lock writeLock = balancedBSTLock.writeLock();
    private final Lock readLock = balancedBSTLock.readLock();

    @SneakyThrows
    public MemTableService(File directory, DrongoDBOptions drongoDBOptions) {
        this.directory = directory;
        this.drongoDBOptions = drongoDBOptions;
        memTable = new MemTable<>(this.drongoDBOptions);
        crashRecoveryService = new CrashRecoveryService(this.directory);
        crashRecoveryService.recoverMemTable(memTable);
        inMemoryIndexService = new InMemoryIndexService(directory, drongoDBOptions);
        ssTableService = new SSTableService(this.directory, memTable, inMemoryIndexService);
    }

    public void putFileEntry(byte[] key, byte[] value) {
        final FileEntry fileEntry = new FileEntry.Builder(key, value).build();
        if (fileEntry.array().length > drongoDBOptions.ssTableThresholdSize) {
            log.error("Entry[key and value] size must be less than 4MB");
            throw new DrongoDBFileEntryException("FileEntry Size is Overflowed, size must be <= 4MB.");
        }
        crashRecoveryService.writeRecoveryLog(fileEntry);
        if (!memTable.put(key, fileEntry)) {
            //write SSTable
            ssTableService.run();
            //clear MemTable and start new SSTableService
            try {
                writeLock.lock();
                memTable = new MemTable<>(drongoDBOptions);
            } finally {
                writeLock.unlock();
            }
            ssTableService = new SSTableService(this.directory, memTable, inMemoryIndexService);
            //clear and write again CrashRecoveryService
            crashRecoveryService.writeNewRecoveryLog(fileEntry);
            //write to MemTable
            try {
                writeLock.lock();
                memTable.put(key, fileEntry);
            } finally {
                writeLock.unlock();
            }
        }
    }
    
    public FileEntry getFileEntry(byte[] key, int retryNumber) {
        //get from memtable
        FileEntry value = null;
        try {
            readLock.lock();
            value = memTable.getFileEntries().get(ByteBuffer.wrap(key));
        } finally {
            readLock.unlock();
        }
        //get from in-memory indexes
        List<SSTable> sstables = new ArrayList<>();
        inMemoryIndexService.getSSTableIndexes().forEach(sstable -> {
            sstables.add(sstable);
        });
        Collections.sort(sstables);
        for (SSTable sstable : sstables) {
            value = sstable.getIndex().getFileEntries().get(ByteBuffer.wrap(key));
            if (value != null) {
                return value;
            }
        }
        if (value == null && retryNumber++ != MAX_RETRY_NUMBER) {
            return getFileEntry(key, retryNumber);
        }
        //get from heap
        
        return value;
    }
    
}
