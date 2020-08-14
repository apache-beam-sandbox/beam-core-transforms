package com.beam.core.transforms;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;


public class CoGroupByKeyTransform {

	public static void main(String[] args) {
		
		List<KV<String,String>> emails = 
				Arrays.asList(KV.of("amy","amy@example.com"),
						KV.of("carl", "carl@example.com"),
						KV.of("julia", "julia@example.com"),
						KV.of("carl", "carl@email.com")
						);
		List<KV<String,String>> phones =
				Arrays.asList(KV.of("amy", "111-222-3333"),
						KV.of("james", "222-333-4444"),
						KV.of("amy", "333-444-5555"),
						KV.of("carl", "444-555-6666"));
		Pipeline p = Pipeline.create();
		
		PCollection<KV<String,String>>
		 	emailsPColl = p.apply("CreateEmailsPColl",Create.of(emails));
		PCollection<KV<String,String>>
			phonesPColl = p.apply("CreatePhonesPColl",Create.of(phones));
		 
		TupleTag<String> emailsTag = new TupleTag();
		TupleTag<String> phonesTag = new TupleTag();
		PCollection<KV<String,CoGbkResult>>	results =
					KeyedPCollectionTuple.of(emailsTag, emailsPColl)
					.and(phonesTag,phonesPColl)
					.apply("joinUsingName", CoGroupByKey.create());
			results.apply("printResults", ParDo.of(new DoFn<KV<String,CoGbkResult>,String>(){
				
				@ProcessElement
				public void processElement(ProcessContext c) {
					KV<String,CoGbkResult> e = c.element();
					Iterable<String> emailsIter = e.getValue().getAll(emailsTag);
					Iterable<String> phonesIter = e.getValue().getAll(phonesTag);
					System.out.println(emailsIter);
					System.out.println(phonesIter);
				}
			}));
			
			p.run().waitUntilFinish();
	}

}
