package com.beam.core.transforms;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import com.beam.core.function.AverageFn;
import com.beam.core.function.DisplayDoubleDoFn;

public class CombineTransformComplex {

	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();
		CoderRegistry coderRegistry = pipeline.getCoderRegistry();
		coderRegistry.registerCoderForClass(Integer.class, BigEndianIntegerCoder.of());
		coderRegistry.registerCoderForClass(Double.class, DoubleCoder.of());
		PCollection<Integer> numbers = pipeline
				.apply(Create.of(1,2,3,4,5,6));
		PCollection<Double> nums = numbers
				.apply("averageNumbers",Combine.globally(new AverageFn()));
		nums.apply(ParDo.of(new DisplayDoubleDoFn()));
		
		pipeline.run().waitUntilFinish();
	}

}
