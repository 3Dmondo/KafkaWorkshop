package com.example.demo;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Program {

    private static final Logger LOGGER = LoggerFactory.getLogger(Program.class);

	public static void main(String[] args) {
		System.console().printf("Please type your name: ");
		System.console().flush();
		String name = System.console().readLine();
		MessageConsumer consumer = new MessageConsumer();
		MessageProducer producer = new MessageProducer(name);
		LOGGER.info("You can start typing your messages");

		Runnable sender = new Runnable() {

			@Override
			public void run() {
				String message = System.console().readLine();
				producer.sendMessage(message);
				this.run();
			}
			
		};

		Runnable receiver = new Runnable() {

			@Override
			public void run() {
				consumer.consume();
				this.run();
			}
			
		};

		ExecutorService executorService = Executors.newFixedThreadPool(2);
		executorService.execute(sender);
		executorService.execute(receiver);
		//executorService.shutdown();
	}

}
