package in.drongo.drongodb.internal.schema;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import com.fasterxml.jackson.databind.JsonNode;

import lombok.SneakyThrows;

public class MetaFile {
    private static final String META_FILE_NAME = "/META";
    private File directory;
    private final File metaFile = new File(directory.getPath() + META_FILE_NAME);
    private final FileChannel metaFileChannel;
    private FileLock fl;
    @SneakyThrows
    public MetaFile(File directory) {
        this.directory = directory;
        metaFileChannel = new RandomAccessFile(metaFile, "rw").getChannel();
    }
    
    @SneakyThrows
    public JsonNode lockMetaFileNode() {
        fl = metaFileChannel.lock();
        if (metaFile.exists() && metaFile.isFile()) {
            return InMemoryIndex.getObjectMapper().readTree(metaFile);
        }
        return null;
    }
    @SneakyThrows
    public void unlockMetaFileNode() {
        fl.release();
        metaFileChannel.close();
    }

}
