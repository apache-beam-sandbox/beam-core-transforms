package com.beam.core.transforms;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import com.beam.core.function.DisplayUpperCaseDoFn;

public class FlattenTransformDemo {

	public static void main(String[] args) {

		List<String> fruits = Arrays.asList("Apple","Banana","Mango");
		List<String> vegetables = Arrays.asList("Carrot","Radish","Potato");
		
		Pipeline pipeline = Pipeline.create();
		PCollection<String> fruitsPColl = pipeline.apply(Create.of(fruits));
		PCollection<String> vegetablesPColl = pipeline.apply(Create.of(vegetables));
		
		PCollectionList<String> pCollList = PCollectionList.of(fruitsPColl)
				.and(vegetablesPColl);
		
		PCollection<String> mergedPColls = pCollList.apply(Flatten
				.<String>pCollections());
		
		mergedPColls.apply("display",ParDo.of(new DisplayUpperCaseDoFn()));
		
		pipeline.run().waitUntilFinish();
	}

}
