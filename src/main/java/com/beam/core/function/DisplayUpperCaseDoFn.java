package com.beam.core.function;

import org.apache.beam.sdk.transforms.DoFn;

public class DisplayUpperCaseDoFn extends DoFn<String, String> {

	@ProcessElement
	public void processElement(@Element String inputStr) {
		System.out.println(inputStr.toUpperCase());
	}
}
