package com.beam.core.transforms;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import com.beam.core.function.ComputeWordLengthFn;

public class ParDoTransformDemo {

	public static void main(String[] args) {
		
		List<String> wordList = Arrays.asList("Apple","Carrot","Healthy","Chips"
				,"Unhealthy");
		
		PipelineOptions options = PipelineOptionsFactory
				.create();
		Pipeline pipeline = Pipeline.create(options);
		PCollection<String> pCollWords = pipeline.apply(Create.of(wordList));
		
		PCollection<Integer> pCollWordLengths =
				pCollWords.apply("computeWordLength", 
						ParDo.of(new ComputeWordLengthFn()));
		
		pipeline.run().waitUntilFinish();
	}
		
}
