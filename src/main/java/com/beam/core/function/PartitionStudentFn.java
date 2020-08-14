package com.beam.core.function;

import org.apache.beam.sdk.transforms.Partition.PartitionFn;
import org.apache.beam.sdk.values.PCollectionList;

import com.beam.core.model.Student;

public class PartitionStudentFn implements PartitionFn<Student> {

	@Override
	public int partitionFor(Student student, int numPartitions) {

		return student.getPercentile() * numPartitions/100;
	}

}
