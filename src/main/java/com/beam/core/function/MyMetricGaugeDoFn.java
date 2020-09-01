package com.beam.core.function;

import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;

public class MyMetricGaugeDoFn extends DoFn<Long, Long> {

	private Gauge gauge = Metrics.gauge("gauge.namespace", "gauge");
	
	@ProcessElement
	public void processElement(ProcessContext pc) {
		Long element = pc.element();
		gauge.set(element);
	}
}
