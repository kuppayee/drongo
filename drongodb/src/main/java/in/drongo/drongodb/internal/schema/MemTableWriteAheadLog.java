package in.drongo.drongodb.internal.schema;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import in.drongo.drongodb.DrongoDBOptions;
import in.drongo.drongodb.util.CodecUtil;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MemTableWriteAheadLog {
    FileChannel walChannel;
    File directory;

    @SneakyThrows
    public MemTableWriteAheadLog(File directory) {
        this.directory = directory;
        walChannel = new RandomAccessFile(new File(directory.getPath() + "/WAL"), "rw").getChannel();
    }

    @SneakyThrows
    public void writeRecoveryLog(FileEntry fileEntry) {
        walChannel.position(walChannel.size());//for append write
        walChannel.write(ByteBuffer.wrap(fileEntry.array()));
    }
    
    @SneakyThrows
    public void clearAndwriteRecoveryLog(FileEntry fileEntry) {
        walChannel.truncate(0);//clear
        walChannel.position(walChannel.size());//for append write
        walChannel.write(ByteBuffer.wrap(fileEntry.array()));
        log.info("New WAL entry created for " + new String(fileEntry.key));
    }
    
    @SneakyThrows
    public void backupRecoveryLog() {
        final FileChannel backupWAL = new RandomAccessFile(new File(directory.getPath() + "/WAL.bak"), "rw").getChannel();
        try {
            backupWAL.transferFrom(walChannel, 0, walChannel.size());
        } finally {
            backupWAL.close();
        }
        log.info("WAL backup done.");
    }
    @SneakyThrows
    public void recoverMemTable(MemTable<byte[], FileEntry> memTable) {
        try {
            final MappedByteBuffer mappedByteBuffer 
            = walChannel.map(FileChannel.MapMode.READ_ONLY, 0, walChannel.size());
            for (long ssTableSize = 0; ssTableSize < walChannel.size();) {
                byte[] id = new byte[32];
                mappedByteBuffer.get(id);
                if (new String(id).trim().isEmpty()) {
                    break;
                }
                byte[] keyLen = new byte[8];
                mappedByteBuffer.get(keyLen);
                byte[] key = new byte[(int) CodecUtil.convertToLongFromByteArray(keyLen)];
                mappedByteBuffer.get(key);
                byte[] valueLen = new byte[8];
                mappedByteBuffer.get(valueLen);
                byte[] value = new byte[(int) CodecUtil.convertToLongFromByteArray(valueLen)];
                mappedByteBuffer.get(value);
                memTable.getFileEntries().put(ByteBuffer.wrap(key), new FileEntry.Builder(key, value).build());
                ssTableSize = mappedByteBuffer.position();
            }
        } finally {
            walChannel.close();
        }
    }
    
    @SneakyThrows
    public void flush() {
        walChannel.force(true);
    }

}
