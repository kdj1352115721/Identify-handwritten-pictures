package com.briup.knn;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PickPic extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new PickPic(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "TopPic20");
		job.setJarByClass(this.getClass());

		job.setMapperClass(PPMapper.class);
		job.setMapOutputKeyClass(PretextSimiTag.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(PPReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		TextInputFormat.addInputPath(job, new Path("/knn_data/train_sort_result"));
		TextOutputFormat.setOutputPath(job, new Path("/knn_data/train_Top20"));

		job.setGroupingComparatorClass(SimiPretextTagGroupComparator.class);

		job.waitForCompletion(true);
		return 0;
	}

	public static class PPMapper extends Mapper<LongWritable, Text, PretextSimiTag, Text> {
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, PretextSimiTag, Text>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] infos = line.split("\t");
			String pretext = infos[0].substring(0, 1);
			String simi = infos[1];
			PretextSimiTag pretextSimiTag = new PretextSimiTag(pretext, Double.parseDouble(simi));
			context.write(pretextSimiTag, new Text("1"));
		}
	}// map

	public static class PPReducer extends Reducer<PretextSimiTag, Text, Text, Text> {
		@Override
		protected void reduce(PretextSimiTag key, Iterable<Text> values,
				Reducer<PretextSimiTag, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			// 1取前20个数据
			int i = 0;
			Map<String, AvgNum> map = new HashMap();
			Iterator<Text> ite = values.iterator();
			while (i < 20) {
				// 遍历value的时候，同步跟新key 的值
				Text next = ite.next();
				String pretext = key.getPretext().toString();
				double simi = key.getSimi().get();
				if (!map.containsKey(pretext)) {
					AvgNum an = new AvgNum(1, simi);
					map.put(pretext, an);
				} else {
					AvgNum old_an = map.get(pretext);
					double old_avg = old_an.getAvg();
					int old_num = old_an.getNumber();
					int new_num = old_num + 1;
					double new_avg = (old_avg * old_num + simi) / new_num;
					AvgNum new_an = new AvgNum(new_num, new_avg);
					map.put(pretext, new_an);
				}
				i++;
			}
			for (Map.Entry<String, AvgNum> en : map.entrySet()) {
				context.write(new Text(en.getKey()),
						new Text(en.getValue().getAvg() + "\t" + en.getValue().getNumber()));
			}

		}

	}// reduce

}
