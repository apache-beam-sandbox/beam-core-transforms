package com.beam.core.model;

import java.io.Serializable;

import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;

@DefaultSchema(JavaBeanSchema.class)
public class Student implements Serializable {

	private String firstName;
	private String lastName;
	private Integer percentile;
	
	@SchemaCreate
	public Student(String firstName, String lastName, Integer percentile) {
		super();
		this.firstName = firstName;
		this.lastName = lastName;
		this.percentile = percentile;
	}
	@Override
	public String toString() {
		return "Student [firstName=" + firstName + ", lastName=" + lastName + ", percentile=" + percentile + "]";
	}
	public String getFirstName() {
		return firstName;
	}
	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}
	public String getLastName() {
		return lastName;
	}
	public void setLastName(String lastName) {
		this.lastName = lastName;
	}
	public Integer getPercentile() {
		return percentile;
	}
	public void setPercentile(Integer percentile) {
		this.percentile = percentile;
	}
}
