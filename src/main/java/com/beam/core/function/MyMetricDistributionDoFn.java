package com.beam.core.function;

import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;

public class MyMetricDistributionDoFn extends DoFn<Integer, Integer> {
	
	private final Distribution distribution =
			Metrics.distribution("distribution.amount", "purchaseAmount");
	
	@ProcessElement
	public void processElement(ProcessContext pc) {
		distribution.update(pc.element());
	}

}
