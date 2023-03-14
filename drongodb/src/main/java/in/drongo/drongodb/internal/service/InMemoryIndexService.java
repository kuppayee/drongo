package in.drongo.drongodb.internal.service;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import in.drongo.drongodb.DrongoDBOptions;
import in.drongo.drongodb.internal.schema.FileEntry;
import in.drongo.drongodb.internal.schema.InMemoryIndex;
import in.drongo.drongodb.internal.schema.MemTable;
import in.drongo.drongodb.internal.schema.MetaFile;
import in.drongo.drongodb.internal.schema.SSTable;
import in.drongo.drongodb.util.CodecUtil;
import in.drongo.drongodb.util.FileUtil;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
@Slf4j
public class InMemoryIndexService {
    private final File directory;
    private final DrongoDBOptions drongoDBOptions;
    private final MetaFile metaFile;
    private InMemoryIndex inMemoryIndex;
    public InMemoryIndexService(File directory, DrongoDBOptions drongoDBOptions, MetaFile metaFile) {
        this.directory = directory;
        this.drongoDBOptions = drongoDBOptions;
        this.metaFile = metaFile;
        inMemoryIndex = new InMemoryIndex(this.directory, this.drongoDBOptions, this.metaFile);
        restoreSSTablesIndexes();
    }

    public void updateIndex(long now, MemTable<byte[], FileEntry> memTable) {
        updateSSTables(now, memTable);
    }

    public void updateMetaFile(long now) {
        updateMetaFileNode(now);
    }
    public BlockingQueue<SSTable> getSSTableIndexes() {
        return inMemoryIndex.getSSTableIndexes();
    }
    
    @SneakyThrows
    public void updateSSTables(long now, MemTable<byte[], FileEntry> memTable) {
        final SSTable sstable = new SSTable();
        sstable.setId(now);
        sstable.setIndex(memTable);
        getSSTableIndexes().put(sstable);
    }

    @SneakyThrows
    public void updateMetaFileNode(long now) {
        final JsonNode root = metaFile.load();
        if (root == null) {
            log.warn("MetaFile not found.");
            return;
        }
        ((ObjectNode) root).put("ref", now + "");
        ((ObjectNode) root).put("currentCount", root.get("currentCount").asInt() + 1);
        metaFile.unload(root);
    }

    @SneakyThrows
    private void restoreSSTablesIndexes() {
        final JsonNode root = metaFile.load();
        if (root == null) {
            log.warn("MetaFile not found.");
            return;
        }
        String tail = root.get("ref").asText();
        if (tail.isEmpty()) {
            return;
        }
        List<Long> sstableNames = new ArrayList<>();
        for (String sstable : FileUtil.getFileNamesStartsWith(new File(directory.getPath()), InMemoryIndex.SSTABLE_FILE_NAME)) {
            sstableNames.add(Long.parseLong(sstable.substring(7)));
        }
        Collections.sort(sstableNames, Comparator.reverseOrder());
        if(!(sstableNames.size() > 0 && tail.length() > 0 && tail.equals(sstableNames.get(0).toString()))) {
            log.warn("META ref for SSTABLE is OutOfSync.");
        }
        for (long sstableName : sstableNames) {
            final var sstable = new SSTable();
            sstable.setId(sstableName);
            sstable.setIndex(restoreSSTableIndex(sstableName));
            getSSTableIndexes().put(sstable);
        }
    }

    @SneakyThrows
    private MemTable<byte[], FileEntry> restoreSSTableIndex(long sstableName) {
        final MemTable<byte[], FileEntry> memTable = new MemTable<>(new DrongoDBOptions());
        final FileChannel ssTableChannel = 
                new RandomAccessFile(new File(directory.getPath() + "/SSTABLE" + sstableName), "rw").getChannel();
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
                memTable.getFileEntries()
                .put(ByteBuffer.wrap(key), new FileEntry.Builder(key, value).build());
                ssTableSize = mappedByteBuffer.position();
            }
        } finally {
            ssTableChannel.close();
        }
        return memTable;
    }

}
