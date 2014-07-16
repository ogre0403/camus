package com.linkedin.camus.etl.kafka.common;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.log4j.Logger;

import java.io.IOException;

public class BinaryRecordWriterProvider implements RecordWriterProvider {
    private static Logger logger = Logger.getLogger(BinaryRecordWriterProvider.class);
    protected NullWritable nullKey = NullWritable.get();

    @Override
    public String getFilenameExtension() {
        return "seq";
    }

    @Override
    public RecordWriter<IEtlKey, CamusWrapper> getDataRecordWriter(
                TaskAttemptContext context,
                String fileName,
                CamusWrapper data,
                FileOutputCommitter committer) throws IOException, InterruptedException {

        Path path = new Path(
                committer.getWorkPath(),
                EtlMultiOutputFormat.getUniqueFile(
                        context, fileName, getFilenameExtension()
                )
        );

        Configuration conf = context.getConfiguration();
        final SequenceFile.Writer writer =
                SequenceFile.createWriter(path.getFileSystem(conf), conf, path,
                        nullKey.getClass(), new BytesWritable().getClass());

        return new RecordWriter<IEtlKey, CamusWrapper>() {
            @Override
            public void write(IEtlKey ignore, CamusWrapper data) throws IOException {
                byte[] record = (byte[]) data.getRecord();
                BytesWritable bw = new BytesWritable(record);
                writer.append(nullKey,bw);
            }

            @Override
            public void close(TaskAttemptContext context) throws IOException, InterruptedException {
                writer.close();
            }
        };

    }
}
