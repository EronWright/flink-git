package org.apache.flink.streaming.connectors.git;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema;
import org.eclipse.jgit.api.Git;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

/**
 * Created by wrighe3 on 4/13/16.
 */
public class PartitionWriterTest {

    private File tmpFile;

    @Before
    public void setUp() throws Exception {
        tmpFile = File.createTempFile("datum", ".avro");
    }

    @After
    public void tearDown() throws Exception {
        tmpFile.deleteOnExit();
    }

    @Test
    public void testReflect() throws Exception {
        Schema schema = ReflectData.get().getSchema(Datum.class);

        DatumWriter<Datum> writer = new ReflectDatumWriter<>(schema);
        try(DataFileWriter<Datum> out = new DataFileWriter<>(writer).create(schema, tmpFile)) {
            for(int actual = 0; actual < 10; actual++) {
                Datum datum = new Datum();
                datum.data = new byte[] { (byte) actual, (byte) 42 };
                out.append(datum);
            }
        }

        DatumReader<Datum> reader = new ReflectDatumReader<>(schema);
        try(DataFileReader<Datum> in = new DataFileReader<>(tmpFile, reader)) {
            int expected = 0;
            for (Datum datum : in) {
                assertArrayEquals(new byte[] { (byte) expected++, (byte) 42 }, datum.data);
            }
        }
    }

    @Test
    public void testKafkaSchemaWriter() throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        TypeInformation<SensorEvent> typeInfo = TypeInformation.of(SensorEvent.class);
        TypeInformationSerializationSchema sinkSchema = new TypeInformationSerializationSchema(typeInfo, env.getConfig());

        SensorEvent evt = new SensorEvent();
        evt.sensorId = 42;
        byte[] data = sinkSchema.serialize(evt);
        assertNotNull(data);
    }

    public static class SensorEvent {
        public int sensorId;
    }

    static class Datum {
        public byte[] data;
    }
}