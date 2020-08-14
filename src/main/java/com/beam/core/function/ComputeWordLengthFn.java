package com.beam.core.function;

import java.util.logging.Logger;

import org.apache.beam.sdk.transforms.DoFn;

public class ComputeWordLengthFn extends DoFn<String, Integer> {

	private static final Logger logger = Logger.getLogger(ComputeWordLengthFn.class.getSimpleName());
	@ProcessElement
	public void process(@Element String word,OutputReceiver<Integer> out) {
		logger.info(word+":"+word.length());
		out.output(word.length());
	}
}
