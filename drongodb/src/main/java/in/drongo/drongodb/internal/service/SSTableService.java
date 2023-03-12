package in.drongo.drongodb.internal.service;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import in.drongo.drongodb.internal.schema.FileEntry;
import in.drongo.drongodb.internal.schema.MemTable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
@Slf4j
public class SSTableService {
    private static final ExecutorService sstableWriteService = Executors.newSingleThreadExecutor(new ThreadFactory() {
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
    private static final Lock sstableWriteLock = new ReentrantLock();

    private final File directory;
    private final MemTable<byte[], FileEntry> memTable;
    private final InMemoryIndexService inMemoryIndexService;

    @SneakyThrows
    public SSTableService(File directory, MemTable<byte[], FileEntry> memTable, InMemoryIndexService inMemoryIndexService) {
        this.directory = directory;
        this.memTable = memTable;
        this.inMemoryIndexService = inMemoryIndexService;
    }

    public static final ExecutorService getSSTableExecutorService() {
        return sstableWriteService;
    }

    public void run() {
        sstableWriteService.execute(() -> writeAndFlush());
    }
    @SneakyThrows
    private void writeAndFlush() {
        sstableWriteLock.lock();
        try {
            final long now = System.nanoTime();
            //every SSTable has separate in-memory index
            inMemoryIndexService.updateIndex(now, memTable);
            // write MemTable into SSTable
            List<ByteBuffer> memTableByteBuffers = new ArrayList<>();
            for(var fileEntry : memTable.getFileEntries().values()) {
                memTableByteBuffers.add(ByteBuffer.wrap(fileEntry.array()));
            }
            // pad '\n' remaining bytes with memTableThresholdSize in balancedBST
            if (memTable.hasRemainingSize()) {
                final ByteBuffer paddingBuffer = ByteBuffer.allocate(memTable.remainingSize());
                Arrays.fill(paddingBuffer.array(), (byte) '\n');
                memTableByteBuffers.add(paddingBuffer);
            }
            final var ssTableChannel = 
                    new RandomAccessFile(new File(directory.getPath() + "/SSTABLE" + now), "rw").getChannel();
            try {
                for(ByteBuffer memTableByteBuffer : memTableByteBuffers) {
                    ssTableChannel.position(ssTableChannel.size());//for append write
                    ssTableChannel.write(memTableByteBuffer);
                }
                ssTableChannel.force(true);//flush
            } finally {
                ssTableChannel.close();
            }
            inMemoryIndexService.updateMetaFile(now);
        } finally {
            sstableWriteLock.unlock();
        }
    }
}
