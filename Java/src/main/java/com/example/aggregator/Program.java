package com.example.aggregator;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Program {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(Program.class);

	public static void main(String[] args) throws InterruptedException, ExecutionException {
        LOGGER.info("press CTRL-C to close.");
        ChatAggregator charCounter = new ChatAggregator();

		ExecutorService executorService = Executors.newFixedThreadPool(2);
		Future<?> future = executorService.submit(() -> {
            charCounter.start();
        });

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			try {
                LOGGER.info("Shutting down");
                charCounter.close();
                future.get();
			} catch (InterruptedException | ExecutionException e) {
                LOGGER.error("Found error: ", e);
			}
		}));
    }

}
