package com.beam.core.function;

import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;

public class CustomKVFunction extends SimpleFunction<String, KV<String,Integer>> {

	@Override
	public KV<String,Integer> apply(String input) {
		
		String[] text = input.split(",");
		if(text.length<2) {
			return null;
		}
		String key=text[0];
		Integer value = Integer.parseInt(text[1]);
		return KV.of(key, value);
	}
}
