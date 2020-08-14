package com.beam.core.transforms;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import com.beam.core.function.ComputeWordLengthFn;
import com.beam.core.function.DisplayUpperCaseDoFn;

public class MultiOutputReceiverDemo {

	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();
		final Integer wordLengthCutOff = 5;
		 TupleTag<String> wordsBelowCutOff =
				new TupleTag<String>() {};
		 TupleTag<String> wordsAboveCutOff =
				new TupleTag<String>() {};
		 TupleTag<String> wordsAtCutOff =
				new TupleTag<String>() {};
		List<String> words = Arrays.asList("Sentence","full",
				"of","words","tough","life","I","am","tougher");
		PCollection<String> wordsPColl = pipeline.apply("getWordsPColl", 
				Create.of(words)).setCoder(AvroCoder.of(String.class));
		
		PCollectionTuple pCollTuple = wordsPColl.apply("groupByCutOff", ParDo.of(new DoFn<String,String>(){
			
			
			  @ProcessElement
			  public void processElement(@Element String word,MultiOutputReceiver out) {
				  
				  if(word.length()>wordLengthCutOff) {
					  out.get(wordsAboveCutOff).output(word);
				  } else if(word.length()==wordLengthCutOff) {
					  out.get(wordsAtCutOff).output(word);
				  } else {
					  out.get(wordsBelowCutOff).output(word);
				  }
			  }
		}).withOutputTags(wordsAtCutOff, TupleTagList.of(wordsAboveCutOff)
				.and(wordsBelowCutOff)));
		
		
		PCollection<String> pCollWordsAtCutOff =pCollTuple.get(wordsAtCutOff);
		pCollWordsAtCutOff.apply("displayWordsUpperCase", 
				ParDo.of(new DisplayUpperCaseDoFn()));
		
		pipeline.run().waitUntilFinish();

	}

}
