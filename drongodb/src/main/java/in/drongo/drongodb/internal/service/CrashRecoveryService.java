package in.drongo.drongodb.internal.service;

import java.io.File;

import in.drongo.drongodb.internal.schema.FileEntry;
import in.drongo.drongodb.internal.schema.MemTable;
import in.drongo.drongodb.internal.schema.MemTableWriteAheadLog;

public class CrashRecoveryService {

    private final MemTableWriteAheadLog memTableWriteAheadLog;
    private File directory;

    public CrashRecoveryService(File directory) {
        this.directory = directory;
        memTableWriteAheadLog = new MemTableWriteAheadLog(this.directory);
    }

    public void writeRecoveryLog(FileEntry fileEntry) {
        memTableWriteAheadLog.writeRecoveryLog(fileEntry);
    }
    
    public void writeNewRecoveryLog(FileEntry fileEntry) {
        memTableWriteAheadLog.clearAndwriteRecoveryLog(fileEntry);
    }
    
    public void recoverMemTable(MemTable<byte[], FileEntry> memTable) {
        memTableWriteAheadLog.recoverMemTable(memTable);
    }

}
