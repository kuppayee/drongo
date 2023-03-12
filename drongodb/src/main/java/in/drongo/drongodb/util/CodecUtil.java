package in.drongo.drongodb.util;

import java.nio.ByteBuffer;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;

import lombok.SneakyThrows;

public class CodecUtil {

    private CodecUtil() {

    }

    public static String encodeHexString(byte[] byteArray) {
        return Hex.encodeHexString(byteArray);
    }

    @SneakyThrows
    public static byte[] decodeHex(String hexStr) {
        return Hex.decodeHex(hexStr);
    }

    public static String generateId(String key) {
        return DigestUtils.sha256Hex(key);
    }

    public static byte[] generateId(byte[] keyAsBytes) {
        return DigestUtils.sha256(keyAsBytes);
    }

    public static String generateIdAsHex(byte[] keyAsBytes) {
        return DigestUtils.sha256Hex(keyAsBytes);
    }
    
    public static byte[] convertToByteArrayFromLong(long n) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(n);
        return buffer.array();
    }

    public static long convertToLongFromByteArray(byte[] byteArray) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(byteArray);
        buffer.flip();
        return buffer.getLong();
    }

}
