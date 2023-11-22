package com.example.demo;

public class DemoApplication {

	public static void main(String[] args) {
		String name = "pippo";
		MessageConsumer consumer = new MessageConsumer();
		MessageProducer producer = new MessageProducer(name);
	}

}
