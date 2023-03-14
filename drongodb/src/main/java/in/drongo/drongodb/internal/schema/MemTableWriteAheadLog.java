package in.drongo.drongodb.internal.schema;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

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
	public void recoverMemTable(MemTable<byte[], FileEntry> memTable) {
		for (long size = 0; size < walChannel.size();) {
			ByteBuffer buffId = ByteBuffer.wrap(new byte[32]);
			walChannel.read(buffId);
			if (new String(buffId.array()).trim().isEmpty()) {
				break;
			}
			ByteBuffer buffKeyLen = ByteBuffer.wrap(new byte[8]);
			walChannel.read(buffKeyLen);
			byte[] key = new byte[(int) CodecUtil.convertToLongFromByteArray(buffKeyLen.array())];
			ByteBuffer buffKey = ByteBuffer.wrap(key);
			walChannel.read(buffKey);
			ByteBuffer buffValueLen = ByteBuffer.wrap(new byte[8]);
			walChannel.read(buffValueLen);
			byte[] value = new byte[(int) CodecUtil.convertToLongFromByteArray(buffValueLen.array())];
			ByteBuffer buffValue = ByteBuffer.wrap(value);
			walChannel.read(buffValue);
			memTable.getFileEntries().put(ByteBuffer.wrap(key), new FileEntry.Builder(key, value).build());
			size = walChannel.position();
		}
	}

	@SneakyThrows
	public void flush() {
		walChannel.force(true);
	}

}
