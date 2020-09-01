package com.beam.metrics;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import com.beam.core.function.MyMetricGaugeDoFn;

public class GaugeMetricDemo {

	public static void main(String[] args) {
		
		List<Long> purchaseQty = Arrays.asList(10L,20L,30L,50L,40L,22L,39L);
		Pipeline pipeline = Pipeline.create();
		PCollection<Long> purchaseQtyPColl =
				pipeline.apply("create pcoll", Create.of(purchaseQty));
		purchaseQtyPColl.apply("apply pardo", ParDo.of(new MyMetricGaugeDoFn()));
		PipelineResult pipelineResult =  pipeline.run();
		MetricQueryResults metricQR = pipelineResult.metrics()
				.queryMetrics(MetricsFilter.builder()
				.addNameFilter(MetricNameFilter.named("gauge.namespace", "gauge"))
				.build());
		
		for(MetricResult<GaugeResult> gaugeResult:metricQR.getGauges()) {
			System.out.println(gaugeResult.getName()+":"
					+gaugeResult.getAttempted()+":"+gaugeResult.getCommitted());
		}
		
		

	}

}
