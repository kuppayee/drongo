package in.drongo.drongodb.internal.schema;

import java.nio.ByteBuffer;
import java.util.Arrays;

import in.drongo.drongodb.util.CodecUtil;

public class FileEntry {
    public final byte[] id;//32 bytes
    public final byte[] keyLen;//8 bytes
    public final byte[] key;
    public final byte[] valueLen;//8 bytes
    public final byte[] value;
    
    private static final char FILE_ENTRY_DELIMITER = '\n';

    public static class Builder {
        private byte[] id;
        private byte[] keyLen;
        private byte[] key;
        private byte[] valueLen;
        private byte[] value;
        
        public Builder(byte[] key, byte[] value) {
            this.id = CodecUtil.generateId(ByteBuffer
                                            .allocate(key.length + value.length + 1)
                                            .put(key).put(value).put((byte) FILE_ENTRY_DELIMITER).array());
            this.keyLen = CodecUtil.convertToByteArrayFromLong(key.length);
            this.valueLen = CodecUtil.convertToByteArrayFromLong(value.length + 1);//extra byte for nl(\n|10)
            this.key = key;
            byte[] endsWithNL = Arrays.copyOf(value, value.length + 1);
            endsWithNL[value.length] = (byte) FILE_ENTRY_DELIMITER;
            this.value = endsWithNL;
        }

        public FileEntry build() {
            return new FileEntry(this);
        }
    }

    private FileEntry(Builder fileEntryBuilder) {
        this.id = fileEntryBuilder.id;
        this.keyLen = fileEntryBuilder.keyLen;
        this.key = fileEntryBuilder.key;
        this.valueLen = fileEntryBuilder.valueLen;
        this.value = fileEntryBuilder.value;
    }
    
    public byte[] array() {
        return ByteBuffer
                .allocate(id.length + keyLen.length + key.length + valueLen.length + value.length)
                .put(id).put(keyLen).put(key).put(valueLen).put(value)
                .array();
    }
}
