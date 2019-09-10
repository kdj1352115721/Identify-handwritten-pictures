package com.briup.knn;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class GetLastResult extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		
		ToolRunner.run(new GetLastResult(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf,"GetLastResult");
		job.setJarByClass(this.getClass());
		
		job.setMapperClass(GLRMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PreTextAvgNum.class);
		
		job.setReducerClass(GLRReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path("/knn_data/train_Top20"));
		TextOutputFormat.setOutputPath(job, new Path("/knn_data/trian_lastResult"));
		
		job.waitForCompletion(true);
		return 0;
	}
	//map 整理数据，整理成自定义类型--符合类型T
	public static class  GLRMapper extends Mapper<LongWritable, Text, Text, PreTextAvgNum>{
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, PreTextAvgNum>.Context context)
				throws IOException, InterruptedException {
			String[] infos = value.toString().split("\t");
			String pretext = infos[0];
			String avg = infos[1];
			String num = infos[2];
			PreTextAvgNum pre = new PreTextAvgNum(pretext,Double.parseDouble(avg),Integer.parseInt(num));
			context.write(new Text("a"), pre);
			
		}
	}//map
	
	//reduce 把一行数据整理成一个整体（数组，键值对，自定义类型--符合类型）,再选择最大值
	public static class GLRReducer extends Reducer<Text, PreTextAvgNum, Text, NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<PreTextAvgNum> values,
				Reducer<Text, PreTextAvgNum, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			Iterator<PreTextAvgNum> ite = values.iterator();
			PreTextAvgNum max = new PreTextAvgNum(ite.next());
			while(ite.hasNext()) {
				PreTextAvgNum current = new PreTextAvgNum(ite.next());
				if(max.getNum().get() < current.getNum().get()) {
					max = new PreTextAvgNum(current);
				}else if(max.getNum().get() == current.getNum().get()) {
					if(max.getAvg().get()<current.getAvg().get()) {
						max= new PreTextAvgNum(current);
					}
				}
			}
			context.write(new Text(max.toString()), NullWritable.get());
		}
	}//reduce
}
