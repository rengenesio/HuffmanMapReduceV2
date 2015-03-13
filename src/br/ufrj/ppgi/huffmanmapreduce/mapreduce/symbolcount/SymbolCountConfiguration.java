package br.ufrj.ppgi.huffmanmapreduce.mapreduce.symbolcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import br.ufrj.ppgi.huffmanmapreduce.Defines;
import br.ufrj.ppgi.huffmanmapreduce.mapreduce.io.ByteCountOutputFormat;
import br.ufrj.ppgi.huffmanmapreduce.mapreduce.io.ByteInputFormat;

public class SymbolCountConfiguration extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		// When implementing tool
		Configuration conf = this.getConf();
		
		// Create job
		Job job = Job.getInstance(conf, "HuffmanSymbolCount");
		job.setJarByClass(SymbolCountConfiguration.class);

		// Setup MapReduce job
		job.setMapperClass(SymbolCountMap.class);
		job.setCombinerClass(SymbolCountReduce.class);
		job.setReducerClass(SymbolCountReduce.class);
		
		// Parse args
		String fileName = args[0];

		// Input
		FileInputFormat.addInputPath(job, new Path(fileName));
		job.setInputFormatClass(ByteInputFormat.class);

		// Specify key / value
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(LongWritable.class);

		// Output
		FileOutputFormat.setOutputPath(job, new Path(fileName + Defines.pathSuffix + Defines.symbolCountSplitsPath));
		job.setOutputFormatClass(ByteCountOutputFormat.class);

		// Execute job and return status (false -> don't show messages)
		return job.waitForCompletion(false) ? 0 : 1;
	}
}