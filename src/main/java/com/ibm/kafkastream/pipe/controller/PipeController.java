package com.ibm.kafkastream.pipe.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ibm.kafkastream.pipe.model.PipeProducerResponse;
import com.ibm.kafkastream.wordcount.controller.WordCountController;

@RestController
public class PipeController {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(WordCountController.class);

//	@Autowired
//	PipeProducerService pipetProducerService;

	@PostMapping(value = "/pipe-producer", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
	public PipeProducerResponse orderProducer(String input) {
		LOGGER.info("##### In Controller for the word count producer");
//		pipeProducerService.emit(input);
		return new PipeProducerResponse("Success");
	}

}
