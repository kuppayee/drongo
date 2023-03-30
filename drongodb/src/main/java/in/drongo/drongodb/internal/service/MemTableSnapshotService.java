package in.drongo.drongodb.internal.service;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import in.drongo.drongodb.DrongoDBOptions;
import in.drongo.drongodb.internal.schema.FileEntry;
import in.drongo.drongodb.internal.schema.MemTableSnapshot;

public class MemTableSnapshotService {
	private MemTableSnapshot memTableSnapshot;
	private final DrongoDBOptions drongoDBOptions;
	private Map<ByteBuffer, FileEntry> mergeTree;
	public MemTableSnapshotService(DrongoDBOptions drongoDBOptions) {
		this.drongoDBOptions = drongoDBOptions;
	}
	
	public Map<ByteBuffer, FileEntry> mergeTree() {
		return mergeTree;
	}
	public void mergeTree(Map<ByteBuffer, FileEntry> mergeTree) {
		this.mergeTree = mergeTree;
	}
	
	public void doSnapshot() {
		memTableSnapshot = new MemTableSnapshot(mergeTree);
		byte[] bytes = memTableSnapshot.takeSnapshot();
		System.out.println("Snapshot>>>>>>>\n" + new String(bytes));
	}

}
