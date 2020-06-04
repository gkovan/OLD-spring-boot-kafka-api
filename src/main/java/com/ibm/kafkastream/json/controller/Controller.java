package com.ibm.kafkastream.json.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ibm.kafkastream.json.model.MyRequest;
import com.ibm.kafkastream.json.service.ProducerService;

@RestController
public class Controller {
	
	@Autowired
	ProducerService producerService;
	
	@GetMapping("/json")
	public String test() {
		MyRequest myRequest = new MyRequest("ramesh", "123456789");
		producerService.sendRecord(myRequest);
		return "hi";
	}

}
