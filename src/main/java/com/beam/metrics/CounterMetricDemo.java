package com.beam.metrics;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import com.beam.core.function.MyMetricCounterDoFn;

public class CounterMetricDemo {

	public static void main(String[] args) {
		
		Pipeline pipeline = Pipeline.create();
		
		List<String> products = Arrays.asList("Doll","Furniture","Chair"
				,"Sofa","Bed","Table","Spoon","Plate");
		
		PCollection<String> productsPColl =
				pipeline.apply("create PColl", Create.of(products));
		
		productsPColl.apply("apply counter metrics", 
				ParDo.of(new MyMetricCounterDoFn()));
		
		PipelineResult pipelineResult = pipeline.run();
		
		MetricQueryResults metricsQR = pipelineResult
				.metrics().queryMetrics(
						MetricsFilter.builder()
						.addNameFilter(MetricNameFilter
						.named("product.namespace", "productCounter"))
						.build());
		
		for(MetricResult<Long> counter : metricsQR.getCounters()) {
			
			System.out.println(counter.getName()+":"+counter.getAttempted());
		}
		
		pipelineResult.waitUntilFinish();
	}

}
