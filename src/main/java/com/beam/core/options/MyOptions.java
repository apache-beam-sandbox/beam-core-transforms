package com.beam.core.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface MyOptions extends PipelineOptions {

	@Description("Input for pipeline")
	@Default.String("myinput")
	String getInput();
	void setInput(String input);
	
	@Description("Output for pipeline")
	@Default.String("myoutput")
	String getOutput();
	void setOutput(String output);
}
