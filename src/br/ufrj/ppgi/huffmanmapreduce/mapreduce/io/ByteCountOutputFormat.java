package br.ufrj.ppgi.huffmanmapreduce.mapreduce.io;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ByteCountOutputFormat<K, V> extends FileOutputFormat<K, V> {

	protected static class ByteRecordWriter<K, V> extends RecordWriter<K, V> {
		FSDataOutputStream out;

		public ByteRecordWriter(FSDataOutputStream out) {
			this.out = out;
		}

		@Override
		public synchronized void write(K key, V value) 
				throws IOException,	InterruptedException {
			boolean test = value == null || value instanceof NullWritable;
			if (!test) {
				IntWritable iw = (IntWritable) key;
				out.writeInt(iw.get());
				LongWritable lw = (LongWritable) value;
				out.writeLong(lw.get());
			}
		}

		@Override
		public synchronized void close(TaskAttemptContext context)
				throws IOException, InterruptedException {
			out.close();
		}
	}

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		String extension = new String();
		Path file = getDefaultWorkFile(job, extension);
		FileSystem fs = file.getFileSystem(job.getConfiguration());
		FSDataOutputStream file_out = fs.create(file);
		return new ByteRecordWriter<K, V>(file_out);
	}
}
