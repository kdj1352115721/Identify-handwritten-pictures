package com.briup.knn;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Similairety extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Similairety(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "SimilarityJob");
		job.setJarByClass(this.getClass());// 告诉集群在那个jar包执行
		// 为job装配mapper
		job.setMapperClass(SMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);


		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		SequenceFileInputFormat.addInputPath(job, new Path("/knn_data/train_bin"));
		TextOutputFormat.setOutputPath(job, new Path("/knn_data/train_result"));

		job.waitForCompletion(true);
		return 0;
	}

	

	public static class SMapper extends Mapper<Text, Text, Text, DoubleWritable> {
		static char[] unkown = new char[400];
		@Override
		protected void setup(Mapper<Text, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			// open开启流
			FSDataInputStream in = fs.open(new Path("/knn_data/unkown"));
			// 接受字符流bufferedReader ，in是字节流
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			String line = reader.readLine();// 读一整行
			unkown = line.toCharArray();// 赋值给数组

		}

		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			char[] array2 = line.toCharArray();
		
			double sum = 0;
			for (int i = 0; i < 400; i++) {
				// 将向量中的字符装换为String,再强转成int
				int x = Integer.parseInt(Character.toString(unkown[i]));
				int t = Integer.parseInt(Character.toString(array2[i]));
				sum += (x - t) * (x - t);
			}
			double distance = 1 / (1 + Math.sqrt(sum));
			context.write(key, new DoubleWritable(distance));
		}

	}// smapper

}
