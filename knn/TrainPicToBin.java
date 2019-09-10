package com.briup.knn;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.PrintWriter;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TrainPicToBin extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new TrainPicToBin(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Path inPath = new Path("/knn_data/train");
		Path outpath = new Path("/knn_data/train_bin");
		// 调用方法
		TrainAllToBin(inPath, outpath, conf);

		return 0;
	}

	public static void TrainAllToBin(Path inpath, Path outpath, Configuration conf) throws Exception {
		FileSystem fs = FileSystem.get(conf);
		// 返回文件列
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(inpath, true);

		SequenceFile.Writer.Option option1 = SequenceFile.Writer.file(outpath);
		SequenceFile.Writer.Option option2 = SequenceFile.Writer.keyClass(Text.class);// 文件前缀名
		SequenceFile.Writer.Option option3 = SequenceFile.Writer.valueClass(Text.class);// 文件的二值化表示
		SequenceFile.Writer writer = SequenceFile.createWriter(conf, option1, option2, option3);

		Text key = new Text();
		Text value = new Text();

		while (listFiles.hasNext()) {
			// 拿到下一个文件
			LocatedFileStatus file = listFiles.next();
			// 把每个文件的名字拿到，除了.png外的
			String name = file.getPath().getName();
			String new_name = name.substring(0, name.indexOf("."));
			key.set(new_name);
			// 开启输入流
			FSDataInputStream in = fs.open(file.getPath());
			// 调用二值化方法
			String bin_line = pinToBin(in);
			value.set(bin_line);
			// 下一文件的二进制码追加
			writer.append(key, value);
		} // while
		writer.close();
	}

	private static String pinToBin(FSDataInputStream in) throws Exception {

		BufferedImage img = ImageIO.read(in);
		StringBuffer sb = new StringBuffer();
		int height = img.getHeight();
		int width = img.getWidth();

		for (int i = 0; i < height; i++) {
			for (int j = 0; j < width; j++) {
				int rgb = img.getRGB(j, i);
				Color gray = new Color(127, 127, 127);
				int garyRgb = gray.getRGB();
				if (rgb < garyRgb) {
					System.out.print("0");
					sb.append("0");
				} else {
					System.out.print("1");
					sb.append("1");
				}
			} // 内
			System.out.println();
		} // 外
		return sb.toString();
	}
}
