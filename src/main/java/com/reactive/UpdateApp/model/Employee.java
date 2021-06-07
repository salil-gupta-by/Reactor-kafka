package com.reactive.UpdateApp.model;


public class Employee {
	
	private int emp_id;
	private String emp_name;
	private String emp_city;
	private String emp_phone;
	private double java_exp;
	private double spring_exp;
	
	public Employee() {
		
	}
	
	
	public Employee(int emp_id, String emp_name, String emp_city, String emp_phone, double java_exp,
			double spring_exp) {
		super();
		this.emp_id = emp_id;
		this.emp_name = emp_name;
		this.emp_city = emp_city;
		this.emp_phone = emp_phone;
		this.java_exp = java_exp;
		this.spring_exp = spring_exp;
	}
	
	
	public int getEmp_id() {
		return emp_id;
	}
	public void setEmp_id(int emp_id) {
		this.emp_id = emp_id;
	}
	public String getEmp_name() {
		return emp_name;
	}
	public void setEmp_name(String emp_name) {
		this.emp_name = emp_name;
	}
	public String getEmp_city() {
		return emp_city;
	}
	public void setEmp_city(String emp_city) {
		this.emp_city = emp_city;
	}
	public String getEmp_phone() {
		return emp_phone;
	}
	public void setEmp_phone(String emp_phone) {
		this.emp_phone = emp_phone;
	}
	public double getJava_exp() {
		return java_exp;
	}
	public void setJava_exp(double java_exp) {
		this.java_exp = java_exp;
	}
	public double getSpring_exp() {
		return spring_exp;
	}
	public void setSpring_exp(double spring_exp) {
		this.spring_exp = spring_exp;
	}
	
	
	
	
	
	
}
