package com.ibm.kafkastream.wordcount.listener;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.ibm.kafkastream.wordcount.service.WordCountKafkaStreamOperator;

/**
 * Servlet context listener to start consumer of the kafka stream that will count the words.
 *  * As the consumer acts as an agent, continuously listening to
 *  * events, we need to start them when the encapsulating app / microservice is successfuly
 *  * started, which is why we have to implement a servlet context listener.
 *  *
 *  *  When the application stops we need to close safely the consumers.
 *
 */
@WebListener
public class WordCountAppContextListener implements ServletContextListener {

	private static final Logger LOGGER = LoggerFactory.getLogger(WordCountAppContextListener.class);
			
	@Autowired
	WordCountKafkaStreamOperator wordCountStreameOperator;

	@Override
	public void contextInitialized(ServletContextEvent sce) {
		LOGGER.info("WordCount Stream Listener started.");
		wordCountStreameOperator.start();	
	}

	@Override
	public void contextDestroyed(ServletContextEvent sce) {
		LOGGER.info("WordCount Stream Listener stopped.");
		wordCountStreameOperator.stop();
	}
}
