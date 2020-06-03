package com.ibm.kafkastream.json.model;

public class MyRequest {
	
	public MyRequest() {
		super();
		// TODO Auto-generated constructor stub
	}
	
	private String name;
	private String requestId;
	
	public MyRequest(String requestName, String requestId) {
		super();
		this.name = requestName;
		this.requestId = requestId;
	}

	
	public String getRequestName() {
		return name;
	}
	public void setRequestName(String requestName) {
		this.name = requestName;
	}
	public String getRequestId() {
		return requestId;
	}
	public void setRequestId(String requestId) {
		this.requestId = requestId;
	}
}
