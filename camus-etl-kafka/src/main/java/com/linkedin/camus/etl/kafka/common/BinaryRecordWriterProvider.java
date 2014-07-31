package com.linkedin.camus.etl.kafka.common;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.coders.KafkaAvroMessageEncoder;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;
import com.linkedin.camus.schemaregistry.SchemaRegistry;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;

public class BinaryRecordWriterProvider implements RecordWriterProvider {
    private static Logger logger = Logger.getLogger(BinaryRecordWriterProvider.class);
    public static final String KAFKA_MESSAGE_CODER_SCHEMA_REGISTRY_CLASS = "kafka.message.coder.schema.registry.class";


    private static SchemaRegistry<Schema> registry = null;
    private static Schema schema = null;
    private DatumReader<GenericRecord> datumReader = null;

    @Override
    public String getFilenameExtension() {
        return ".seq";
    }

    @Override
    public RecordWriter<IEtlKey, CamusWrapper> getDataRecordWriter(
                TaskAttemptContext context,
                String fileName,
                CamusWrapper data,
                FileOutputCommitter committer) throws IOException, InterruptedException {

        if (registry == null) {
            String registryClass = context.getConfiguration().get(KAFKA_MESSAGE_CODER_SCHEMA_REGISTRY_CLASS);
            System.out.println(registryClass);
            try {
                registry =  (SchemaRegistry<Schema>) Class.forName(registryClass).newInstance();
            } catch (InstantiationException e) {
                logger.error(stackToString(e));
            } catch (IllegalAccessException e) {
                logger.error(stackToString(e));
            } catch (ClassNotFoundException e) {
                logger.error(stackToString(e));
            }
        }

        schema = registry.getLatestSchemaByTopic("DUMMY_LOG_3").getSchema();

        datumReader = new GenericDatumReader<GenericRecord>(schema);

        Path path = new Path(
                committer.getWorkPath(),
                EtlMultiOutputFormat.getUniqueFile(
                        context, fileName, getFilenameExtension()
                )
        );

        Configuration conf = context.getConfiguration();
        final SequenceFile.Writer writer =
                SequenceFile.createWriter(path.getFileSystem(conf), conf, path,
                        new Text().getClass(), new BytesWritable().getClass());

        return new RecordWriter<IEtlKey, CamusWrapper>() {
            BinaryDecoder d1 = DecoderFactory.get().binaryDecoder(new byte[0],null);
            Text k = new Text();
            @Override
            public void write(IEtlKey ignore, CamusWrapper data) throws IOException {
                byte[] record = (byte[]) data.getRecord();
                Decoder decoder = DecoderFactory.get().binaryDecoder(record,d1);
                GenericRecord rr = datumReader.read(null,decoder);
                k.set(rr.get("filename").toString());
                ByteBuffer bf = (ByteBuffer)rr.get("raw");
                byte[] ba = new byte[bf.capacity()];
                bf.get(ba,0,ba.length);
                BytesWritable bw = new BytesWritable(ba);
                writer.append(k,bw);
            }

            @Override
            public void close(TaskAttemptContext context) throws IOException, InterruptedException {
                writer.close();
            }
        };

    }

    private String stackToString(Exception e){
        StringWriter errors = new StringWriter();
        e.printStackTrace(new PrintWriter(errors));
        return errors.toString();
    }
}
