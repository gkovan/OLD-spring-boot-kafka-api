package com.ibm.kafkastream.pipe.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PipeStreamServiceTest {
	
	private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> outputTopic;
	PipeStreamService pipeStreamService = new PipeStreamService();
	
    @Before
	public void setup() {

		testDriver = new TopologyTestDriver(pipeStreamService.getTopology(), pipeStreamService.getProperties());
		inputTopic = testDriver.createInputTopic(PipeStreamService.INPUT_TOPIC,  new StringSerializer(), new StringSerializer());
		outputTopic = testDriver.createOutputTopic(PipeStreamService.OUTPUT_TOPIC, new StringDeserializer(), new StringDeserializer());
	}
	

    @After
	public void tearDown() {
	    testDriver.close();
	}
	
	@Test
	public void outputValueShouldBeSameAsInputValue() {
		// given
		String inputValue = "gerry";
		inputTopic.pipeInput("anykey", inputValue);
		
		// when 
		// stream code as described by testDriver is running
		
		// then
		assertEquals(outputTopic.readValue(),inputValue);
		assertTrue(outputTopic.isEmpty());
	}
	
	@Test
	public void outputSentenceValueShouldBeSameAsInputSentenceValue() {
		// given
		String inputValue = "hello my name is gerry";
		inputTopic.pipeInput("anykey", inputValue);
		
		// when 
		// stream code as described by testDriver is running
		
		// then
		assertEquals(outputTopic.readValue(),inputValue);
		assertTrue(outputTopic.isEmpty());
	}

}
