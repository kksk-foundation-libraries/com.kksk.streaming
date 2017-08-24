package com.kksk.streaming.starter;

import javax.naming.Context;

import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.PollingConsumer;
import org.apache.camel.RoutesBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.processor.UnitOfWorkProducer;
import org.apache.camel.spi.Registry;
import org.apache.camel.util.CamelContextHelper;
import org.apache.camel.util.ServiceHelper;

class CamelContextSupport {
	private final CamelContext _context;

	CamelContextSupport() {
		_context = new DefaultCamelContext();
	}

	CamelContextSupport(Context jndiContext) {
		_context = new DefaultCamelContext(jndiContext);
	}

	CamelContextSupport(Registry registry) {
		_context = new DefaultCamelContext(registry);
	}

	PollingConsumer createDirectConsumer(String uri) throws Exception {
		Endpoint ep = CamelContextHelper.getMandatoryEndpoint(_context, uri);
		ServiceHelper.startService(ep);
		PollingConsumer consumer = ep.createPollingConsumer();
		_context.addService(consumer, true, true);
		return consumer;
	}

	UnitOfWorkProducer createDirectProducer(String uri) throws Exception {
		Endpoint ep = CamelContextHelper.getMandatoryEndpoint(_context, uri);
		ServiceHelper.startService(ep);
		UnitOfWorkProducer producer = new UnitOfWorkProducer(ep.createProducer());
		_context.addService(producer, true, true);
		return producer;
	}

	void addRoutes(RoutesBuilder builder) throws Exception {
		_context.addRoutes(builder);
	}

	static Message consume(PollingConsumer directConsumer) {
		return directConsumer.receive().getIn();
	}

	static void produce(UnitOfWorkProducer directProducer, Message message) throws Exception {
		Exchange exchange = directProducer.createExchange();
		exchange.setIn(message);
		directProducer.process(exchange);
	}

	void start() throws Exception {
		_context.start();
	}

	void stop() throws Exception {
		_context.stop();
	}
}
