package com.beam.core;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

/**
 * Hello world!
 *
 */
public class PCollectionDemo 
{
    public static void main( String[] args )
    {
        PipelineOptions options =
        		PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        List<String> lines = Arrays.asList("Apple","Mango","Banana","Guava");
        PCollection<String> pcoll = pipeline
        		.apply(Create.of(lines)).setCoder(StringUtf8Coder.of());
		
		  pcoll.apply("readLines", ParDo.of(new DoFn<String,String>() {
		  
		  @ProcessElement public void process(@Element String line) {
			  			System.out.println(line);
		  } }));
		 
        
        pipeline.run().waitUntilFinish();
    }
}
