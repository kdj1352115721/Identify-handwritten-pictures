package com.briup.knn;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.PrintWriter;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Upload extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Upload(), args);
	}
	@Override
	public int run(String[] arg0) throws Exception {

		Configuration conf = getConf();
		Path inpath = new Path(conf.get("inpath"));
		Path outpath = new Path("/knn_data/unkown");
		putToHdfs(inpath, outpath, conf);
		return 0;
	}
	public static void putToHdfs(Path inpath, Path outpath, Configuration conf) throws Exception {
		LocalFileSystem local = FileSystem.getLocal(conf);
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = local.open(inpath);
		FSDataOutputStream out = fs.create(outpath);
		BinTest(in, out, true);
	}
	

	public static void BinTest(FSDataInputStream inpath, FSDataOutputStream outpath, boolean close)
			throws Exception, IOException {

		BufferedImage img = ImageIO.read(inpath);
		PrintWriter outwrite = new PrintWriter(outpath);
		int height = img.getHeight();
		int width = img.getWidth();
		
 		for (int i = 0; i < height; i++) {
			for (int j = 0; j < width; j++) {
				int rgb = img.getRGB(j, i);
				Color gray = new Color(127, 127, 127);
				int garyRgb = gray.getRGB();
				if (rgb < garyRgb) {
					System.out.print("0");
					outwrite.write("0");
					outwrite.flush();
				} else {
					System.out.print("1");
					outwrite.write("1");
					outwrite.flush();
				}
			} // 内
			
		} // 外
 		
		if (close) {
			inpath.close();
			outwrite.close();
		}
	}

}
