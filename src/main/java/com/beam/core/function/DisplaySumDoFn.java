package com.beam.core.function;

import org.apache.beam.sdk.transforms.DoFn;

public class DisplaySumDoFn extends DoFn<Integer, Integer> {

	@ProcessElement
	public void myProcessElement(@Element Integer sum) {
		System.out.println("Sum:"+sum);
	}
}
