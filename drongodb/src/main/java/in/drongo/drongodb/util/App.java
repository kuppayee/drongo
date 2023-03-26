package in.drongo.drongodb.util;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) throws Exception {
		
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

