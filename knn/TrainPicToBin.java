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
		// ���÷���
		TrainAllToBin(inPath, outpath, conf);

		return 0;
	}

	public static void TrainAllToBin(Path inpath, Path outpath, Configuration conf) throws Exception {
		FileSystem fs = FileSystem.get(conf);
		// �����ļ���
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(inpath, true);

		SequenceFile.Writer.Option option1 = SequenceFile.Writer.file(outpath);
		SequenceFile.Writer.Option option2 = SequenceFile.Writer.keyClass(Text.class);// �ļ�ǰ׺��
		SequenceFile.Writer.Option option3 = SequenceFile.Writer.valueClass(Text.class);// �ļ��Ķ�ֵ����ʾ
		SequenceFile.Writer writer = SequenceFile.createWriter(conf, option1, option2, option3);

		Text key = new Text();
		Text value = new Text();

		while (listFiles.hasNext()) {
			// �õ���һ���ļ�
			LocatedFileStatus file = listFiles.next();
			// ��ÿ���ļ��������õ�������.png���
			String name = file.getPath().getName();
			String new_name = name.substring(0, name.indexOf("."));
			key.set(new_name);
			// ����������
			FSDataInputStream in = fs.open(file.getPath());
			// ���ö�ֵ������
			String bin_line = pinToBin(in);
			value.set(bin_line);
			// ��һ�ļ��Ķ�������׷��
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
			} // ��
			System.out.println();
		} // ��
		return sb.toString();
	}
}
