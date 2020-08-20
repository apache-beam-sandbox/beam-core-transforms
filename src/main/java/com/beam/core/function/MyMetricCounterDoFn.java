package com.beam.core.function;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;

public class MyMetricCounterDoFn extends DoFn<String, String> {
	
	private final Counter counter = Metrics.counter("product.namespace", 
			"productCounter");
	
	@ProcessElement
	public void processElement(@Element String product,ProcessContext pc) {
		
		counter.inc();
		pc.output(product);
	}

}
