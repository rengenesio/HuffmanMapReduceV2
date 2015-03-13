package br.ufrj.ppgi.huffmanmapreduce.mapreduce.decoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import br.ufrj.ppgi.huffmanmapreduce.Defines;
import br.ufrj.ppgi.huffmanmapreduce.mapreduce.io.ByteInputFormat;
import br.ufrj.ppgi.huffmanmapreduce.mapreduce.io.BytesWritableEncoder;
import br.ufrj.ppgi.huffmanmapreduce.mapreduce.io.EncoderOutputFormat;

public class DecoderConfiguration extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		// When implementing tool
		Configuration conf = this.getConf();
		
		// Parse args
		String fileName = args[0];
		
		// Configuration to be accessed by map classes
		conf.set("fileName", fileName);
		System.out.println(conf.get("mapred.map.child.java.opts"));
		conf.set("mapred.map.child.java.opts", "-Xmx1024m");
		System.out.println(conf.get("mapred.map.child.java.opts"));
		// Create job
		Job job = Job.getInstance(conf, "HuffmanDecoderMR");
		job.setJarByClass(DecoderConfiguration.class);

		// Setup MapReduce job do not specify the number of Reducer
		job.setMapperClass(DecoderMap.class);
		job.setNumReduceTasks(0);
		
		// Input
		FileInputFormat.addInputPath(job, new Path(fileName + Defines.pathSuffix + Defines.compressedSplitsPath));
		job.setInputFormatClass(ByteInputFormat.class);

		// Specify key / value
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(BytesWritableEncoder.class);

		// Output
		FileOutputFormat.setOutputPath(job, new Path(fileName + Defines.pathSuffix + Defines.decompressedPath));
		job.setOutputFormatClass(EncoderOutputFormat.class);
	
		// Execute job and return status
		return job.waitForCompletion(false) ? 0 : 1;
	}
	
}
