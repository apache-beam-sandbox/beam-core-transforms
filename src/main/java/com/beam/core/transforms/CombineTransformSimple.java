package com.beam.core.transforms;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import com.beam.core.function.DisplaySumDoFn;
import com.beam.core.function.SumIntsFunction;

public class CombineTransformSimple {

	public static void main(String[] args) {


		Pipeline pipeline = Pipeline.create();
		PCollection<Integer> numsPColl = pipeline
				.apply(Create.of(10,20,30,40));
		PCollection<Integer> sumPColl = numsPColl.apply("sumUpNumbers", Combine.globally(new SumIntsFunction()));
		sumPColl.apply("displaySum", ParDo.of(new DisplaySumDoFn()));
		
		pipeline.run().waitUntilFinish();
	}

}
