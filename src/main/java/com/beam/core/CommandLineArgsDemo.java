package com.beam.core;

import java.util.logging.Logger;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import com.beam.core.options.MyOptions;

public class CommandLineArgsDemo {

	private static final Logger logger = Logger.getLogger(CommandLineArgsDemo.class.getSimpleName());
	public static void main(String[] args) {
		
		PipelineOptionsFactory.register(MyOptions.class);
		PipelineOptions options =
				PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
		
		for(String arg:args) {
			logger.info(arg);
		}
		
		Pipeline pipeline = Pipeline.create(options);
		pipeline.run(options).waitUntilFinish();
	}

}
