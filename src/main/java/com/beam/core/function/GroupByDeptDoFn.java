package com.beam.core.function;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class GroupByDeptDoFn extends DoFn<KV<String, Iterable<Integer>>,String> {

	@ProcessElement
	public void process(ProcessContext pc) {
		String dept = pc.element().getKey(); // First element needs to be KV input
		Iterable<Integer> students = pc.element().getValue();// String is the output
		int studentsByDept=0;
		for(Integer s:students) {
			studentsByDept+=s;
		}
		
		pc.output(dept+"="+studentsByDept);
	}
}
