package com.beam.core.transforms;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;

import com.beam.core.function.CustomKVFunction;
import com.beam.core.function.GroupByDeptDoFn;

public class GroupByTransform {

	public static void main(String[] args) {

		PipelineOptions options = PipelineOptionsFactory.create();
		Pipeline pipeline = Pipeline.create(options);
		
		pipeline.apply(TextIO.read().from("/src/main/resources/GroupByInput.txt"))
		.apply("convertToKV", MapElements.via(new CustomKVFunction()))
		.apply("groupByDept",GroupByKey.<String,Integer>create())
		.apply("studentsByDept",ParDo.of(new GroupByDeptDoFn()))
		.apply(TextIO.write().to("./src/main/resources/GroupByOutput.txt")
		.withoutSharding());
		
		pipeline.run().waitUntilFinish();
	}

}
