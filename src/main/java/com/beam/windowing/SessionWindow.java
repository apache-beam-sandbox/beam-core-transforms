package com.beam.windowing;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

public class SessionWindow {

	public static void main(String[] args) {
		List<String> productList = Arrays.asList("Doll","Flower"
				,"Chair","Table","Car","Battery","Bike","Duster"
				,"Pen","Pencil","Eraser","TV","Audio System");
		
		Pipeline pipeline = Pipeline.create();
		
		PCollection<String> productPColl = pipeline
				.apply("create product PColl", Create.of(productList));
		
		PCollection<String> slidingWindowPColl = productPColl
				.apply("session window", Window.<String>
				into(Sessions.withGapDuration(Duration.standardSeconds(26)))
				.triggering(AfterWatermark.pastEndOfWindow())
				.withAllowedLateness(Duration.millis(500000))
				.discardingFiredPanes());
				
		
		slidingWindowPColl.apply("display",ParDo.of(new DoFn<String,String>() {
			
			@ProcessElement
			public void processElement(@Element String product,IntervalWindow window) {
				
				System.out.println(product+" :Gap duration:"+(window.end().getMillis()-window.start().getMillis())/1000+" seconds");
			}
		}));
		
		pipeline.run().waitUntilFinish();
	}

}
