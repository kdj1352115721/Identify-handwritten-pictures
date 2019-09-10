package com.briup.knn;

import java.awt.Color;
import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import javax.imageio.ImageIO;

public class BinTest {
	public static void main(String[] args) throws Exception, IOException {
		BufferedImage img = ImageIO.read(new FileInputStream("src/0_15.png"));
		int height = img.getHeight();
		int width = img.getWidth();
		for (int i = 0; i < height; i++) {
			for (int j = 0; j < width; j++) {
				int rgb = img.getRGB(j, i);
				Color gray = new Color(127, 127, 127);
				int garyRgb = gray.getRGB();
				if(rgb<garyRgb) {
					System.out.print("0");
				}else {
					System.out.print("1");
				}
			}//ÄÚ
			System.out.println();
		}//Íâ
	}
	

}
