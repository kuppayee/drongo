package in.drongo.drongodb.internal.schema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import in.drongo.drongodb.util.avro.FileEntryAvro;
import in.drongo.drongodb.util.avro.MemTableAvro;

public class MemTableSnapshot {
	private final Map<ByteBuffer, FileEntry> mergeTree;
	
	MemTableSnapshot(Map<ByteBuffer, FileEntry> mergeTree) {
		this.mergeTree = mergeTree;
	}
	
	public byte[] takeSnapshot() throws Exception {
		final DatumWriter<MemTableAvro> writer = new SpecificDatumWriter<>(MemTableAvro.class);
		final ByteArrayOutputStream stream = new ByteArrayOutputStream();
		final Encoder jsonEncoder = EncoderFactory.get().binaryEncoder(stream, null);
		writer.write(buildMemTableAvroFromMergeTree(), jsonEncoder);
		jsonEncoder.flush();
		return stream.toByteArray();
	}
	
	public MemTableAvro getSnapshot(byte[] data) throws Exception {
	    final DatumReader<MemTableAvro> reader = new SpecificDatumReader<>(MemTableAvro.class);
	    final ByteArrayInputStream stream = new ByteArrayInputStream(data);
        return reader.read(null, DecoderFactory.get().binaryDecoder(stream, null));
	}
	
	private MemTableAvro buildMemTableAvroFromMergeTree() {
		final MemTableAvro memTableAvro = new MemTableAvro();
		final Map<CharSequence, FileEntryAvro> bst = new TreeMap<>();
		mergeTree.forEach((k, v) -> {
			bst.put(new String(k.array()), new FileEntryAvro(ByteBuffer.wrap(v.id), 
					ByteBuffer.wrap(v.keyLen), ByteBuffer.wrap(v.key), ByteBuffer.wrap(v.valueLen), ByteBuffer.wrap(v.value)));
		});
		memTableAvro.put("balancedBST", bst);
		return memTableAvro;
	}
	
	
	protected static void generateMemTableSchemaAvro() {
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
	}

}
