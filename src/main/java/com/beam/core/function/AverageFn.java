package com.beam.core.function;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.transforms.Combine.CombineFn;

public class AverageFn extends CombineFn<Integer, AverageFn.Accum, Double> {
	//Test

	@DefaultCoder(AvroCoder.class)
	public static class Accum {
		int sum = 0;
		int count = 0;
		
	}

	@Override
	public Accum createAccumulator() {

		return new Accum();
	}

	@Override
	public Accum addInput(Accum accum, Integer input) {
		accum.sum = accum.sum + input;
		accum.count++;
		return accum;
	}

	@Override
	public Accum mergeAccumulators(Iterable<Accum> accums) {
		Accum mergedAccum = createAccumulator();
		for(Accum accum:accums) {
			mergedAccum.sum = mergedAccum.sum+accum.sum;
			mergedAccum.count= mergedAccum.count+accum.count;
		}
		return mergedAccum;
	}

	@Override
	public Double extractOutput(Accum accum) {
		return (double)accum.sum/accum.count;
	}
	
}
