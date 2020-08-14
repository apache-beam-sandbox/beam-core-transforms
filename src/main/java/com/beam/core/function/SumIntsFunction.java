package com.beam.core.function;

import org.apache.beam.sdk.transforms.SerializableFunction;

public class SumIntsFunction implements SerializableFunction<Iterable<Integer>, Integer> {

	@Override
	public Integer apply(Iterable<Integer> numbers) {
		Integer sum = 0;
			for(int n:numbers) {
				sum+=n;
			}
		return sum;
	}

}
