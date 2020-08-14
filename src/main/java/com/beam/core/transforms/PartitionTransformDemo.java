package com.beam.core.transforms;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import com.beam.core.function.PartitionStudentFn;
import com.beam.core.function.StudentDoFn;
import com.beam.core.model.Student;

public class PartitionTransformDemo {

	private static final transient Logger logger = Logger.getLogger(PartitionTransformDemo.class.getSimpleName());
	public static void main(String[] args) {
		
		Student s1 = new Student("Pradeep","Ravindran",90);
		Student s2 = new Student("Karthik","Narayan",95);
		Student s3 = new Student("Hemanth","Swamy",80);
		Student s4 = new Student("Ntini","Haasan",70);
		Student s5 = new Student("Shanmukha","Haasan",75);
		
		Student s6 = new Student("Pradeep","Ravindran",90);
		Student s7 = new Student("Karthik","Narayan",95);
		Student s8 = new Student("Hemanth","Swamy",80);
		Student s9 = new Student("Ntini","Haasan",70);
		Student s10 = new Student("Shanmukha","Haasan",75);
		
		Student s11 = new Student("Pradeep","Ravindran",90);
		Student s12 = new Student("Karthik","Narayan",95);
		Student s13 = new Student("Hemanth","Swamy",80);
		Student s14 = new Student("Ntini","Haasan",70);
		Student s15 = new Student("Shanmukha","Haasan",75);
		
		List<Student> studentList = Arrays.asList(s1,s2,s3,s4,s5,s6,s7,
				s8,s9,s10,s11,s12,s13,s14,s15);
		
		Pipeline pipeline = Pipeline.create();
		PCollection<Student> studentPColl = pipeline
				.apply(Create.of(studentList));
		
		PCollectionList<Student> studentPCollList = studentPColl
				.apply(Partition.of(5,new PartitionStudentFn()));
		 
		logger.info("studentPCollList size:"+studentPCollList.size());
		studentPColl = studentPCollList.get(4); // Get the fourth partition
		studentPColl.apply("showStudents", ParDo.of(new StudentDoFn()));
		
		pipeline.run().waitUntilFinish();
	}

}
