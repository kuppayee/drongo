package in.drongo.drongodb.util;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import in.drongo.drongodb.DrongoDBOptions;
import in.drongo.drongodb.internal.schema.FileEntry;
import in.drongo.drongodb.internal.schema.MemTable;
import lombok.SneakyThrows;

public class Todo {
    public static void main(String[] args) throws Exception {
    	MemTable<byte[], FileEntry> memTable = new MemTable<byte[], FileEntry>(new DrongoDBOptions());
    	recoverMemTable(memTable);
    	//System.out.println(memTable);
    }
    
    @SneakyThrows
    public static void recoverMemTable(MemTable<byte[], FileEntry> memTable) {
        try {
        	var walChannel = new RandomAccessFile(new File("C:/demo/WAL"), "rw").getChannel();
        	for (long ssTableSize = 0; ssTableSize < walChannel.size();) {
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
                System.out.println(new String(key) + "=" + new String(value));
                ssTableSize = walChannel.position();
            }
        } finally {
            //redoLog.close();
        }
    }
    
    public static void recoverMemTable0(MemTable<byte[], FileEntry> memTable) throws Exception {
    	var walChannel = new RandomAccessFile(new File("C:/demo/SSTABLE99724900696000"), "rw").getChannel();
        try {
            final MappedByteBuffer mappedByteBuffer 
            = walChannel.map(FileChannel.MapMode.READ_ONLY, 0, walChannel.size());
            for (long ssTableSize = 0; ssTableSize < walChannel.size();) {
                byte[] id = new byte[32];
                mappedByteBuffer.get(id);
                if (new String(id).trim().isEmpty()) {
                    break;
                }
                System.out.println("id?" + new String(id));
                byte[] keyLen = new byte[8];
                mappedByteBuffer.get(keyLen);
                System.out.println("keyLen?" + CodecUtil.convertToLongFromByteArray(keyLen));
                byte[] key = new byte[(int) CodecUtil.convertToLongFromByteArray(keyLen)];
                mappedByteBuffer.get(key);
                System.out.println("keyLen?" + new String(key));
                byte[] valueLen = new byte[8];
                mappedByteBuffer.get(valueLen);
                System.out.println("valueLen?" + new String(valueLen));
                byte[] value = new byte[(int) CodecUtil.convertToLongFromByteArray(valueLen)];
                mappedByteBuffer.get(value);
                System.out.println("value?" + new String(value));
                memTable.getFileEntries().put(ByteBuffer.wrap(key), new FileEntry.Builder(key, value).build());
                ssTableSize = mappedByteBuffer.position();break;
            }
        } finally {
            walChannel.close();
        }
    }
    
}
