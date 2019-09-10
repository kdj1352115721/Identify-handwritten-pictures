package com.briup.knn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SimiPretextTagGroupComparator extends WritableComparator {
	public SimiPretextTagGroupComparator() {
		super(PretextSimiTag.class,true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		PretextSimiTag s1 = (PretextSimiTag)a;
		PretextSimiTag s2 = (PretextSimiTag)b;
		return s1.getTag().compareTo(s2.getTag());
	}
}
