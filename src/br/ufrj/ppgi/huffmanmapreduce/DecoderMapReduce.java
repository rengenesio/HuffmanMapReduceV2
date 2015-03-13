package br.ufrj.ppgi.huffmanmapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import br.ufrj.ppgi.huffmanmapreduce.mapreduce.decoder.DecoderConfiguration;



public class DecoderMapReduce {
	String fileName;

	public DecoderMapReduce(String fileName)
			throws Exception {
		this.fileName = fileName;
		
		String[] s = new String[1];
		s[0] = this.fileName;
		
		// MAPREDUCE SYMBOL COUNT
		ToolRunner.run(new Configuration(), new DecoderConfiguration(), s);
		// END MAPREDUCE SYMBOL COUNT
	}
}
