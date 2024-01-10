package com.example.chat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Program {

    private static final Logger LOGGER = LoggerFactory.getLogger(Program.class);

	public static void main(String[] args) {
		LOGGER.info("Please type your name: ");
		String name = System.console().readLine();
		MessageConsumer consumer = new MessageConsumer();
		MessageProducer producer = new MessageProducer(name);
		LOGGER.info("You can start typing your messages");

		CountDownLatch senderLatch = new CountDownLatch(1);

		Runnable sender = () -> {
			boolean isRunning = true;
			while (isRunning) {
				String message = System.console().readLine();
				if (message == null) {
					senderLatch.countDown();
					isRunning = false;
				} else {
					producer.sendMessage(message);
				}
			}
		};

		Runnable receiver = () -> {
			boolean isRunning = true;
			while (isRunning) {
				if (senderLatch.getCount() == 0) {
					isRunning = false;
				} else {
					for (ConsumerRecord<String, String> cr : consumer.consume().toList()) {
						LOGGER.debug("Consumed message {} from Kafka", cr.value());
						LOGGER.info(">>> {}: {}", cr.key(), cr.value());
					}
				}
			}
		};

		try {
			ExecutorService executorService = Executors.newFixedThreadPool(2);
			Future<?> receiverFut = executorService.submit(receiver);
			Future<?> senderFut = executorService.submit(sender);
			senderFut.get();
			receiverFut.get();
			LOGGER.info("Shutting down...");
			executorService.shutdown();
			executorService.awaitTermination(60, TimeUnit.SECONDS);
			consumer.close();
			producer.close();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
	}

}
