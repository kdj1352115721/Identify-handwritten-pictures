package com.briup.knn;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class PicSort extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new PicSort(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "picsort");
		job.setJarByClass(this.getClass());// 告诉集群在那个jar包执行

		job.setMapperClass(PCMapper.class);
		job.setMapOutputKeyClass(PretextSimiTag.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		TextInputFormat.addInputPath(job, new Path("/knn_data/train_result/part-r-00000"));
		TextOutputFormat.setOutputPath(job, new Path("/knn_data/train_sort_result"));
		
		job.setGroupingComparatorClass(SimiPretextTagGroupComparator.class);
		
		job.waitForCompletion(true);
		return 0;
	}

	public static class PCMapper extends Mapper<LongWritable, Text, PretextSimiTag, NullWritable> {
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, PretextSimiTag, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] splits = line.split("\t");
			String pretext = splits[0];
			String simi = splits[1];
			PretextSimiTag pretextTag = new PretextSimiTag(pretext,Double.parseDouble(simi));
			context.write(pretextTag, NullWritable.get());
		}
	}// map
	

}
