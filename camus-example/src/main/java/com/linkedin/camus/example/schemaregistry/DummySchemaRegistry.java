package com.linkedin.camus.example.schemaregistry;

import com.linkedin.camus.example.records.DummyLog3;

import com.linkedin.camus.schemaregistry.AvroMemorySchemaRegistry;

import java.nio.ByteBuffer;

/**
 * This is a little dummy registry that just uses a memory-backed schema
 * registry to store two dummy Avro schemas. You can use this with
 * camus.properties
 */
public class DummySchemaRegistry extends AvroMemorySchemaRegistry {
//	public DummySchemaRegistry(Configuration conf) {
	public DummySchemaRegistry() {
		super();
        super.register("DUMMY_LOG_3", DummyLog3.newBuilder()
                                        .setFilename("")
                                        .setRaw(ByteBuffer.wrap(new byte[0])).build().getSchema());
	}
}
