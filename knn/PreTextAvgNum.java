package com.briup.knn;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class PreTextAvgNum implements Writable {
	private Text pretext = new Text();
	private DoubleWritable avg = new DoubleWritable();
	private IntWritable num = new IntWritable();
	
	
	public PreTextAvgNum() {
		
	}
	
	
/*	public PreTextAvgNum(line) {
		line.toString.split("\t"); 
		this.pretext = new Text(info[0]);
	}*/
	public PreTextAvgNum(PreTextAvgNum pan) {
		this.pretext = new Text(pan.getPretext().toString());
		this.avg = new DoubleWritable(pan.getAvg().get());
		this.num = new IntWritable(pan.getNum().get());
	}
	
	public PreTextAvgNum(String pretext,double avg, int num) {
		this.pretext= new Text(pretext);
		this.avg = new DoubleWritable(avg);
		this.num = new IntWritable(num);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		pretext.readFields(in);
		avg.readFields(in);
		num.readFields(in);
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
			pretext.write(out);
			avg.write(out);
			num.write(out);
	}


	public Text getPretext() {
		return pretext;
	}


	public void setPretext(Text pretext) {
		this.pretext = new Text(pretext.toString());
	}


	public DoubleWritable getAvg() {
		return avg;
	}


	public void setAvg(DoubleWritable avg) {
		this.avg = new DoubleWritable(avg.get());
	}


	public IntWritable getNum() {
		return num;
	}


	public void setNum(IntWritable num) {
		this.num = new IntWritable(num.get());
	}


	@Override
	public String toString() {
		return "最后的结果"+pretext.toString();
	}
	
	
	
	
}
