package com.beam.core.transforms;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import com.beam.core.function.ComputeWordLengthFn;
import com.beam.core.function.MaxIntegerFn;

public class SideInputsDemo {

	public static void main(String[] args) {
		
		List<String> words = Arrays.asList("Apple","Mango","Banana","Custard Apple"
				,"Jackfruit","Water Melon","Kiwi","Guava","aaaaaaaaaabbbbbbbbbbbbccccccccccc");
		
		Pipeline pipeline = Pipeline.create();
		
		PCollection<String> wordsPColl = pipeline
				.apply("createPColl", Create.of(words));
		
		PCollection<Integer> wordLengthsPColl = wordsPColl
				.apply("computeWordLength", ParDo.of(new ComputeWordLengthFn()));
		
		final PCollectionView<Integer> maxWordLengthCutOffView =
				wordLengthsPColl.apply("maxLengthCutOff",Combine.globally(new MaxIntegerFn()).asSingletonView());
		
		
		wordsPColl.apply("wordsBelowCutOff", ParDo.of(new DoFn<String,String>() {
			
			
			@ProcessElement
			public void processElement(@Element String word,ProcessContext pc) {
				Integer cutOffLength = (Integer) pc.sideInput(maxWordLengthCutOffView);
				if(word.length()<cutOffLength) {
					System.out.println(word+":cutOffLength:"+cutOffLength);
				}
			}
		}).withSideInputs(maxWordLengthCutOffView));
		
		pipeline.run().waitUntilFinish();
		}
	}


