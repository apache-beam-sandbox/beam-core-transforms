package com.beam.core.function;

import java.util.logging.Logger;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollection;

import com.beam.core.model.Student;

public class StudentDoFn extends DoFn<Student, String> {

	private static final transient Logger logger = Logger.getLogger(StudentDoFn.class.getSimpleName());
	@ProcessElement
	public void processElement(ProcessContext c) {
		
				Student student = c.element();
				logger.info(student.toString());
			
		};
	}

