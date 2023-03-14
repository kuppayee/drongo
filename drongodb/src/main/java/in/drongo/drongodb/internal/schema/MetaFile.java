package in.drongo.drongodb.internal.schema;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
@Slf4j
public class MetaFile {
	private static final String META_FILE_NAME = "/META";
	private static Lock fileLock = new ReentrantLock();
	private File metaFile;
	private FileChannel metaFileChannel;
	private static final ObjectMapper mapper = getObjectMapper();

	private MetaFile() {
	}

	@SneakyThrows
	public void initMetaFile(File directory) {
		metaFile = new File(directory.getPath() + META_FILE_NAME);
		boolean isMetaFileExist = metaFile.exists() && metaFile.isFile();
		metaFileChannel = new RandomAccessFile(metaFile, "rw").getChannel();
		if (isMetaFileExist) {
			return;
		}
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
		if (new File(directory.getPath() + "/HEAP").createNewFile()) {
			log.debug("HEAP File initialized.");
		}
	}

	@SneakyThrows
	public JsonNode load() {
		fileLock.lock();
		try {
			return mapper.readTree(metaFile);
		} finally {
			fileLock.unlock();
		}
	}

	@SneakyThrows
	public void unload(JsonNode root) {
		fileLock.lock();
		try {
			mapper.writeValue(metaFile, root);
		} finally {
			fileLock.unlock();
		}
	}


	private static ObjectMapper getObjectMapper() {
		var mapper = new ObjectMapper();
		mapper.registerModule(new JavaTimeModule());
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		return mapper;
	}

	private static class Holder {
		private static final MetaFile INSTANCE = new MetaFile();
	}
	public static MetaFile getInstance() {
		return Holder.INSTANCE;
	}
}
