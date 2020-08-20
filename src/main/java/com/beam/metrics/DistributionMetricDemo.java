package com.beam.metrics;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import com.beam.core.function.MyMetricDistributionDoFn;

public class DistributionMetricDemo {

	public static void main(String[] args) {

		Pipeline pipeline = Pipeline.create();
		List<Integer> purchaseAmounts = Arrays.asList(10,20,30,40);
		
		PCollection<Integer> purchaseAmtPColl =
				pipeline.apply("create PColl", Create.of(purchaseAmounts));
		purchaseAmtPColl.apply("distributionMetric", 
				ParDo.of(new MyMetricDistributionDoFn()));
		PipelineResult pipelineResult = pipeline.run();
		
		MetricQueryResults metricQueryResult = pipelineResult.metrics()
					  .queryMetrics(MetricsFilter.builder()
					  .addNameFilter(MetricNameFilter.named("distribution.amount", "purchaseAmount"))
					  .build());
		for(MetricResult<DistributionResult> dr:metricQueryResult.getDistributions()) {
			System.out.println(dr.getName()+"::"+dr.getCommitted());
		}

		pipelineResult.waitUntilFinish();
	}

}
