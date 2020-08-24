package com.beam.windowing;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

public class SlidingTimeWindow {

	public static void main(String[] args) {
		List<String> productList = Arrays.asList("Doll","Flower"
				,"Chair","Table","Car","Battery","Bike","Duster"
				,"Pen","Pencil","Eraser","TV","Audio System");
		
		Pipeline pipeline = Pipeline.create();
		PCollection<String> productPColl = pipeline
				.apply("create product PColl", Create.of(productList));
		
		// Sliding windows have data for particular interval 
		// 30 seconds worth of data firing every 5 seconds
		// hence the sliding windows overlap
		PCollection<String> slidingWindowPColl = productPColl
				.apply("sliding window", Window.<String>
				into(SlidingWindows.of(Duration.standardSeconds(30))
				.every(Duration.standardSeconds(5))));
		
		slidingWindowPColl.apply("display",ParDo.of(new DoFn<String,String>() {
			
			@ProcessElement
			public void processElement(@Element String product,IntervalWindow intervalWindow) {
				
				System.out.println(product+":"+(intervalWindow.end().getMillis()-intervalWindow.start().getMillis())/1000+" seconds");
			}
		}));
		
		pipeline.run().waitUntilFinish();
	}

}
