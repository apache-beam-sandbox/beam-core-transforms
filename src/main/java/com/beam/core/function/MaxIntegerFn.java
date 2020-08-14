package com.beam.core.function;

import java.util.Iterator;

import org.apache.beam.sdk.transforms.SerializableFunction;

public class MaxIntegerFn implements SerializableFunction<Iterable<Integer>,Integer> {

	@Override
	public Integer apply(Iterable<Integer> wordLengths) {
		
		Integer max = 0;
		Iterator<Integer> it = wordLengths.iterator();
				while(it.hasNext()) {
					Integer item = it.next();
					if(item>max) {
						max = item;
					}
				}
		return max;
	}

}
