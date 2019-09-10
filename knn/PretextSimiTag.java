package com.briup.knn;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class PretextSimiTag implements WritableComparable<PretextSimiTag> {
	private Text pretext = new Text();
	private DoubleWritable simi = new DoubleWritable();
	private Text tag = new Text("1");

	public PretextSimiTag() {
		super();
	}

	public PretextSimiTag(String pretext, double simi) {
		this.pretext = new Text(pretext);
		this.simi = new DoubleWritable(simi);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		pretext.readFields(in);
		simi.readFields(in);
		tag.readFields(in);

	}

	@Override
	public void write(DataOutput out) throws IOException {
		pretext.write(out);
		simi.write(out);
		tag.write(out);
	}
	
	@Override//·ÖÇø
	public int compareTo(PretextSimiTag o) {
		return o.simi.compareTo(this.simi);
	}

	public Text getPretext() {
		return pretext;
	}

	public void setPretext(Text pretext) {
		this.pretext = new Text(pretext.toString());
	}

	public DoubleWritable getSimi() {
		return simi;
	}

	public void setSimi(DoubleWritable simi) {
		this.simi = new DoubleWritable(simi.get());
	}

	public Text getTag() {
		return tag;
	}

	public void setTag(Text tag) {
		this.tag = new Text(tag.toString());
	}

	@Override
	public String toString() {
		return this.pretext.toString() + "\t" + this.simi.get();
	}

}
