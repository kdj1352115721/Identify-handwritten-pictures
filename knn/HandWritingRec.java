package com.briup.knn;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.commons.math3.analysis.function.Sigmoid;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.nfs.nfs3.request.PATHCONF3Request;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NewApplication;

import com.briup.knn.GetLastResult.GLRMapper;
import com.briup.knn.GetLastResult.GLRReducer;
import com.briup.knn.PicSort.PCMapper;
import com.briup.knn.PickPic.PPMapper;
import com.briup.knn.PickPic.PPReducer;
import com.briup.knn.Similairety.SMapper;

public class HandWritingRec extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new HandWritingRec(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		// 清理上一次运行的目录文件
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path("/knn_data/train_result"),true);
		fs.delete(new Path("/knn_data/train_sort_result"),true);
		fs.delete(new Path("/knn_data/train_Top20"),true);
		fs.delete(new Path("/knn_data/trian_lastResult"),true);
		fs.delete(new Path("/knn_data/unkown"), false);// 删文件

		// 1 待识别图片，二值化上传到hdfs
		Path inpath = new Path("/unkown/unkown.png");
		Path outpath = new Path("/knn_data/unkown");
		Upload.putToHdfs(inpath, outpath, conf);
		// 2 计算相似度
		Job simiJob = Job.getInstance(conf, "SimilarityJob");
		simiJob.setJarByClass(this.getClass());// 告诉集群在那个jar包执行
		simiJob.setMapperClass(SMapper.class);
		simiJob.setMapOutputKeyClass(Text.class);
		simiJob.setMapOutputValueClass(DoubleWritable.class);
		simiJob.setInputFormatClass(SequenceFileInputFormat.class);
		simiJob.setOutputFormatClass(TextOutputFormat.class);
		SequenceFileInputFormat.addInputPath(simiJob, new Path("/knn_data/train_bin"));
		TextOutputFormat.setOutputPath(simiJob, new Path("/knn_data/train_result"));

		// 3 按照相似度进行排序
		Job sortJob = Job.getInstance(conf, "picsort");
		sortJob.setJarByClass(this.getClass());
		sortJob.setMapperClass(PCMapper.class);
		sortJob.setMapOutputKeyClass(PretextSimiTag.class);
		sortJob.setMapOutputValueClass(NullWritable.class);
		sortJob.setInputFormatClass(TextInputFormat.class);
		sortJob.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(sortJob, new Path("/knn_data/train_result/part-r-00000"));
		TextOutputFormat.setOutputPath(sortJob, new Path("/knn_data/train_sort_result"));
		sortJob.setGroupingComparatorClass(SimiPretextTagGroupComparator.class);

		// 4 knn 统计平均相似度和标签个数
		Job top20Job = Job.getInstance(conf, "TopPic20");
		top20Job.setJarByClass(this.getClass());
		top20Job.setMapperClass(PPMapper.class);
		top20Job.setMapOutputKeyClass(PretextSimiTag.class);
		top20Job.setMapOutputValueClass(Text.class);
		top20Job.setReducerClass(PPReducer.class);
		top20Job.setOutputKeyClass(Text.class);
		top20Job.setOutputValueClass(Text.class);
		top20Job.setInputFormatClass(TextInputFormat.class);
		top20Job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(top20Job, new Path("/knn_data/train_sort_result"));
		TextOutputFormat.setOutputPath(top20Job, new Path("/knn_data/train_Top20"));
		top20Job.setGroupingComparatorClass(SimiPretextTagGroupComparator.class);

		// 5 找到相似度最高的那个最大值的，并获得文件名；
		Job lastresultJob = Job.getInstance(conf, "GetLastResult");
		lastresultJob.setJarByClass(this.getClass());
		lastresultJob.setMapperClass(GLRMapper.class);
		lastresultJob.setMapOutputKeyClass(Text.class);
		lastresultJob.setMapOutputValueClass(PreTextAvgNum.class);
		lastresultJob.setReducerClass(GLRReducer.class);
		lastresultJob.setOutputKeyClass(Text.class);
		lastresultJob.setOutputValueClass(NullWritable.class);
		lastresultJob.setInputFormatClass(TextInputFormat.class);
		lastresultJob.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(lastresultJob, new Path("/knn_data/train_Top20"));
		TextOutputFormat.setOutputPath(lastresultJob, new Path("/knn_data/trian_lastResult"));

		// 装配工作流
		ControlledJob job1 = new ControlledJob(simiJob.getConfiguration());

		ControlledJob job2 = new ControlledJob(sortJob.getConfiguration());

		ControlledJob job3 = new ControlledJob(top20Job.getConfiguration());

		ControlledJob job4 = new ControlledJob(lastresultJob.getConfiguration());

		job2.addDependingJob(job1);
		job3.addDependingJob(job2);
		job4.addDependingJob(job3);

		JobControl control = new JobControl("handWritingRec");

		control.addJob(job1);
		control.addJob(job2);
		control.addJob(job3);
		control.addJob(job4);

		Thread t = new Thread(control);
		t.start();
		while (!control.allFinished()) {

		}
		System.out.println("图片识别完毕！！！");
		FSDataInputStream open = fs.open(new Path("/knn_data/trian_lastResult/part-r-00000"));

		BufferedReader reader = new BufferedReader(new InputStreamReader(open));
		String resultline = reader.readLine();
		System.out.println("结果：" + resultline);

		reader.close();
		fs.close();
		System.exit(0);
		return 0;

	}

}
