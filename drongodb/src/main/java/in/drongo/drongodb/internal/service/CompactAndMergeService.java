package in.drongo.drongodb.internal.service;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.fasterxml.jackson.databind.JsonNode;

import in.drongo.drongodb.DrongoDBOptions;
import in.drongo.drongodb.internal.schema.FileEntry;
import in.drongo.drongodb.internal.schema.MemTable;
import in.drongo.drongodb.internal.schema.MetaFile;
import in.drongo.drongodb.internal.schema.SSTable;
import in.drongo.drongodb.util.CodecUtil;
import in.drongo.drongodb.util.FileUtil;
import in.drongo.drongodb.util.Todo;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
@Slf4j
public class CompactAndMergeService {

    //private static ExecutorService ssTableExecutorService = SSTableService.getSSTableExecutorService();
    private static final ScheduledExecutorService compactAndMergeService = 
            Executors.newScheduledThreadPool(1, new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread thread = new Thread(r);
                    thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                        public void uncaughtException(Thread t, Throwable e) {
                            log.error("", e);
                        }
                    });
                    return thread;
                }
            });
    private static final Lock writeLock = new ReentrantLock(true);
    private volatile PartialWriteRecoveryService partialWriteRecoveryService;
    private volatile CheckSumService checkSumService;
    private volatile File directory;
    private volatile DrongoDBOptions drongoDBOptions;
    private volatile String currentSSTableName;


    public CompactAndMergeService(File directory, DrongoDBOptions drongoDBOptions) {
        this.directory = directory;
        this.drongoDBOptions = drongoDBOptions;
        run();
    }

    private void run() {
        compactAndMergeService
        .scheduleWithFixedDelay(() -> compactAndMerge(), 
                DrongoDBOptions.COMPACT_AND_MERGE_SCHEDULED_INITIAL_DELAY, DrongoDBOptions.COMPACT_AND_MERGE_SCHEDULED_DELAY, TimeUnit.SECONDS);
    }

    private void compactAndMerge() {
        if (writeLock.tryLock()) {
            try {
                if (getSSTableCount() < 4) {//size-tiered compaction strategy (STCS)
                    return;
                }
                //do compactAndMerge
                final List<Long> sstablesNames = 
                        FileUtil.getFileNamesStartsWith(directory, "SSTABLE", Long.parseLong(currentSSTableName));
                Collections.sort(sstablesNames, Comparator.reverseOrder());
                //merge newer to older update meta file
                Map<ByteBuffer, FileEntry> mergeTree = new TreeMap<>();
                compactAndMergeSSTable(mergeTree, sstablesNames);
                //write mergeTree to HEAP.merge
                //delete SSTables
                //merge HEAP.merge and HEAP file, write to new HEAP file
                //delete HEAP.merge and old HEAP file
            } finally {
                writeLock.unlock();
            }
        }
    }

    private int getSSTableCount() {
        final MetaFile metaFile = new MetaFile(directory);
        try {
            final JsonNode root = metaFile.lockMetaFileNode();
            if (root == null) {
                log.warn("MetaFile in Use.");
                return 0;
            }
            currentSSTableName = root.get("ref").asText();
            return root.get("currentCount").asInt();
        } finally {
            metaFile.unlockMetaFileNode();
        }
    }

    @SneakyThrows
    private void compactAndMergeSSTable(Map<ByteBuffer, FileEntry> mergeTree, List<Long> sstablesNames) {
        //base condition
        if (sstablesNames.isEmpty()) {
            return;
        }
        mergeTree.putAll(getSSTable(sstablesNames.remove(0)).getIndex().getFileEntries());
        compactAndMergeSSTable(mergeTree, sstablesNames);
    }

    @SneakyThrows
    private SSTable getSSTable (long now) {
        final MemTable<byte[], FileEntry> ssTableTree = new MemTable<>(new DrongoDBOptions());
        final FileChannel ssTableChannel = 
                new RandomAccessFile(new File(directory.getPath() + "/SSTABLE" + now), "rw").getChannel();
        try {
            final MappedByteBuffer mappedByteBuffer 
            = ssTableChannel.map(FileChannel.MapMode.READ_ONLY, 0, ssTableChannel.size());
            for (long ssTableSize = 0; ssTableSize < ssTableChannel.size();) {
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
                ssTableTree.getFileEntries()
                .put(ByteBuffer.wrap(key), new FileEntry.Builder(key, value).build());
                ssTableSize = mappedByteBuffer.position();
            }
        } finally {
            ssTableChannel.close();
        }
        SSTable sstable = new SSTable();
        sstable.setId(now);
        sstable.setIndex(ssTableTree);
        return sstable;
    }

}
