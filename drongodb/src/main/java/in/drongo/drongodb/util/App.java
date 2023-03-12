package in.drongo.drongodb.util;

import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.codec.binary.Hex;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) throws Exception {

		String json = Files.readString(Paths.get("src/petstore.json"));
		json = "{\"name\":\"Error\",\"message\":\"hello\"}";
		//System.out.println(json.getBytes().length);
		String record = "1xyzjjjjjjjjjj" + json;
		MessageBufferPacker messageBufferPacker = MessagePack.newDefaultBufferPacker();
		messageBufferPacker.packBinaryHeader(record.getBytes().length);
		messageBufferPacker.writePayload(record.getBytes());
		String hexStr = Hex.encodeHexString(messageBufferPacker.toByteArray());
		
		MessageUnpacker messageUnpacker = MessagePack.newDefaultUnpacker(Hex.decodeHex(hexStr));
		int hi = messageUnpacker.unpackBinaryHeader();
		byte[] payLoad = messageUnpacker.readPayload((int) hi);
		System.out.println(new String(payLoad));
		
		long address = com.sun.jna.Native.malloc(11);
		//static native void write(long addr, byte[] buf, int index, int length);
		//com.sun.jna.Native.toByteArray(address, "seetharaman".getBytes(), 0, 11);
		com.sun.jna.Native.free(address);
		
		StdC lib = StdC.INSTANCE;
		com.sun.jna.Pointer p = lib.malloc(1024);
		p.setMemory(0L, 10L, (byte) 97);
		byte[] bytes = p.getByteArray(0, 10);
		System.out.println(new String(bytes));
		lib.free(p);
		
	}
	
	public interface CMath extends com.sun.jna.Library { 
	    double cosh(double value);
	}
	
	public interface StdC extends com.sun.jna.Library {
	    StdC INSTANCE = com.sun.jna.Native.load(com.sun.jna.Platform.isWindows()?"msvcrt":"c", StdC.class);
	    com.sun.jna.Pointer malloc(long n);
	    void free(com.sun.jna.Pointer p);
	}

}

