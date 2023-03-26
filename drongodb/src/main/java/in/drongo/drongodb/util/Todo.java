package in.drongo.drongodb.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import in.drongo.drongodb.util.avro.FileEntryAvro;
import in.drongo.drongodb.util.avro.MemTableAvro;

public class Todo {
	public static void main(String[] args) throws Exception {
		/*public final byte[] id;//32 bytes
        public final byte[] keyLen;//8 bytes
        public final byte[] key;
        public final byte[] valueLen;//8 bytes
        public final byte[] value;*/
		Schema schema = SchemaBuilder.record("MemTableAvro")
				.namespace("in.drongo.drongodb.util.avro")
				.fields()
				.name("balancedBST")
				.type().map()
				.values(SchemaBuilder.record("FileEntryAvro")
						.namespace("in.drongo.drongodb.util.avro")
						.fields()
						.name("id")
						.type().bytesType().noDefault()
						.name("keyLen")
						.type().bytesType().noDefault()
						.name("key")
						.type().bytesType().noDefault()
						.name("valueLen")
						.type().bytesType().noDefault()
						.name("value")
						.type().bytesType().noDefault()
						.endRecord())
				.noDefault()
				.endRecord();
		System.out.println(schema);
		MemTableAvro avro = new MemTableAvro();
		FileEntryAvro fe = new FileEntryAvro();
		ByteBuffer bb = ByteBuffer.allocate(4);
		bb.putInt(5);bb.flip();
		fe.setId(bb);
		ByteBuffer bb1 = ByteBuffer.allocate(4);
		bb1.putInt(5);bb1.flip();
		fe.setKeyLen(bb1);
		ByteBuffer bb2 = ByteBuffer.allocate(4);
		bb2.putInt(5);bb2.flip();
		fe.setKey(bb2);
		ByteBuffer bb3 = ByteBuffer.allocate(4);
		bb3.putInt(5);bb3.flip();
		fe.setValueLen(bb3);
		ByteBuffer bb4 = ByteBuffer.allocate(4);
		bb4.putInt(5);bb4.flip();
		fe.setValue(bb4);
		Map<Integer, FileEntryAvro> map = new TreeMap<>(); 
		map.put(1, fe);map.put(2, fe);
		avro.put("balancedBST", map);
		byte[] ret = serializeAvroHttpRequestJSON(avro, schema);
		System.out.println("ret?" + new String(ret));
		MemTableAvro ds = deSerializeAvroHttpRequestJSON(ret);
		((Map)ds.get("balancedBST")).forEach((key, value)-> {
			System.out.println("::::" + key);
		});
	}

	public static byte[] serializeAvroHttpRequestJSON(MemTableAvro request, Schema schema) throws Exception{

		DatumWriter<MemTableAvro> writer = new SpecificDatumWriter<>(MemTableAvro.class);
		byte[] data = new byte[0];
		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		Encoder jsonEncoder = null;
		try {
			jsonEncoder = EncoderFactory.get().binaryEncoder(stream, null);
			writer.write(request, jsonEncoder);
			jsonEncoder.flush();
			data = stream.toByteArray();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return data;
	}
	
	public static MemTableAvro deSerializeAvroHttpRequestJSON(byte[] data) throws Exception {
	    DatumReader<MemTableAvro> reader
	     = new SpecificDatumReader<>(MemTableAvro.class);
	    ByteArrayInputStream stream = new ByteArrayInputStream(data);
	    Decoder decoder = null;
	    try {
	        decoder = DecoderFactory.get().binaryDecoder(stream, null);
	        return reader.read(null, decoder);
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
	    return null;
	}

}
