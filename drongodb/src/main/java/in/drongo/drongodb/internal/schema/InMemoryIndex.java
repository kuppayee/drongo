package in.drongo.drongodb.internal.schema;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import in.drongo.drongodb.DrongoDBOptions;
import in.drongo.drongodb.util.CodecUtil;
import in.drongo.drongodb.util.FileUtil;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InMemoryIndex {
    private static BlockingQueue<SSTable> ssTableIndexes;
    private static final ObjectMapper mapper = getObjectMapper();
    private static final String META_FILE_NAME = "/META";
    private static final String HEAP_FILE_NAME = "/HEAP";
    private static final String SSTABLE_FILE_NAME = "SSTABLE";

    private final File directory;
    private final DrongoDBOptions drongoDBOptions;
    public InMemoryIndex(File directory, DrongoDBOptions drongoDBOptions) {
        this.directory = directory;
        this.drongoDBOptions = drongoDBOptions;
        initSSTables();
    }

    @SneakyThrows
    private void initSSTables() {
        ssTableIndexes = new LinkedBlockingDeque<>(4);
        final File metaFile = new File(directory.getPath() + META_FILE_NAME);
        if(metaFile.exists() && metaFile.isFile()) { 
            restoreSSTablesIndex();
            ssTableIndexes.forEach(System.out::println);
            return;
        }
        final var metaFileChannel = 
                new RandomAccessFile(new File(directory.getPath() + META_FILE_NAME), "rw").getChannel();
        try {
            final InputStream is = getClass().getClassLoader().getResourceAsStream("META");
            final ByteArrayOutputStream byteHolder = new ByteArrayOutputStream();
            final byte[] b = new byte[128];
            for(int len = 0; (len = is.read(b)) > -1; ) {
                byteHolder.write(b, 0, len);
            }
            metaFileChannel.write(ByteBuffer.wrap(byteHolder.toByteArray()));
        } finally {
            metaFileChannel.close();
        }
        if (new File(directory.getPath() + HEAP_FILE_NAME).createNewFile()) {
            log.debug("HEAP File initialized.");
        }
    }

    public BlockingQueue<SSTable> getSSTableIndexes() {
        return ssTableIndexes;
    }

    @SneakyThrows
    public void updateSSTables(long now, MemTable<byte[], FileEntry> memTable) {
        final SSTable sstable = new SSTable();
        sstable.setId(now);
        sstable.setIndex(memTable);
        ssTableIndexes.put(sstable);
    }

    @SneakyThrows
    public void updateMetaFileNode(long now) {
        final MetaFile metaFile = new MetaFile(directory);
        final JsonNode root = metaFile.lockMetaFileNode();
        if (root == null) {
            log.warn("MetaFile in Use.");
            return;
        }
        try {
            ((ObjectNode) root).put("ref", now + "");
            ((ObjectNode) root).put("currentCount", root.get("currentCount").asInt() + 1);
            final FileChannel fc = 
                    new RandomAccessFile(new File(directory.getPath() + "/META"), "rw").getChannel();
            mapper.writeValue(new File(directory.getPath() + META_FILE_NAME), root);
        } finally {
            metaFile.unlockMetaFileNode();
        }
    }

    @SneakyThrows
    private void restoreSSTablesIndex() {
        final MetaFile metaFile = new MetaFile(directory);
        final JsonNode root = metaFile.lockMetaFileNode();
        if (root == null) {
            log.warn("MetaFile in Use.");
            return;
        }
        String tail = "";
        try {
            tail = root.get("ref").asText();
            if (tail.isEmpty()) {
                return;
            }
        }finally {
            metaFile.unlockMetaFileNode();
        }
        List<Long> sstableNames = new ArrayList<>();
        for (String sstable : FileUtil.getFileNamesStartsWith(new File(directory.getPath()), SSTABLE_FILE_NAME)) {
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
            ssTableIndexes.put(sstable);
        }
    }


    public static ObjectMapper getObjectMapper() {
        var mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper;
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
