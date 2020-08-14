package com.beam.core.function;

import java.io.Serializable;

import org.apache.beam.sdk.transforms.DoFn;

public class DisplayDoubleDoFn extends DoFn<Double, Double> implements Serializable {

	@ProcessElement
	public void processElement(@Element Double average) {
		System.out.println("Average:"+average);
	}
}
