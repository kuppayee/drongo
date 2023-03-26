package in.drongo.drongodb.internal;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import in.drongo.drongodb.DrongoDB;
import in.drongo.drongodb.exception.DrongoDBFileLockException;
import in.drongo.drongodb.internal.schema.MetaFile;
import in.drongo.drongodb.internal.service.CompactAndMergeService;
import in.drongo.drongodb.internal.service.CrashRecoveryService;
import in.drongo.drongodb.internal.service.MemTableService;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * Factory method to create DrongoDB instance
 *
 */
@Slf4j
@ToString
public class DefaultDrongoDB extends DrongoDB {

    private FileLock dbFileLock;
    private Lock writeLock;
    private MetaFile metaFile;
    private CrashRecoveryService crashRecoveryService;
    private MemTableService memTableService;
    private CompactAndMergeService compactAndMergeService;
    

    @Override
    @SneakyThrows
    protected DrongoDB createDrongoDB() {
        try {

            drongoDBOptions.validateDrongoDBOptions();
            writeLock = new ReentrantLock();
            dbFileLock = tryDBFileLock();
            metaFile = MetaFile.getInstance();
            metaFile.initMetaFile(directory);
            crashRecoveryService = new CrashRecoveryService(directory);
            memTableService = new MemTableService(directory, drongoDBOptions, metaFile, crashRecoveryService);
            compactAndMergeService = new CompactAndMergeService(directory, drongoDBOptions, metaFile, crashRecoveryService);

        } catch (Throwable t) {
            if (dbFileLock != null) {
                dbFileLock.close();
            }
            throw t;
        }
        return this;
    }

    public byte[] get(byte[] key) {
        return memTableService.getFileEntry(key, 0).value;
    }

    public void put(byte[] key, byte[] value) {
        writeLock.lock();
        try {
            // add to memtable
            memTableService.putFileEntry(key, value);
        } finally {
            writeLock.unlock();
        }
    }

    public void delete(byte[] key) {
        writeLock.lock();
        try {

        } finally {
            writeLock.unlock();
        }
    }

    @SneakyThrows
    public void close() {
        writeLock.lock();
        try {
            if (dbFileLock != null) {
                dbFileLock.close();
            }
        } finally {
            writeLock.unlock();
        }
    }

    private FileLock tryDBFileLock() {
        int count = 0;
        int tryLimt = 2;
        while (this != null) {
            try {
                FileLock fileLock = FileChannel
                        .open(directory.toPath().resolve(".DBLOCK"), StandardOpenOption.CREATE, StandardOpenOption.WRITE)
                        .tryLock();
                if (fileLock == null) {
                    log.error("Error while opening DrongoDB. Another process holding lock.");
                    throw new DrongoDBFileLockException("Another process holding lock.");
                }
    
                return fileLock;
            } catch (OverlappingFileLockException | IOException e) {
                if (++count == tryLimt) {
                    log.error("Error while opening DrongoDB. Another process holding lock!");
                    throw new DrongoDBFileLockException("Another process already using db lock!");
                }
                log.error("Error while opening DrongoDB. re-trying...");
            }
        }
        return null;
    }

}
