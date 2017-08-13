package com.kksk.streaming.starter;

import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.PollingConsumer;
import org.apache.camel.util.CamelContextHelper;
import org.apache.camel.util.ServiceHelper;

/**
 * Consumer utility for Camel
 */
class CamelConsumer {
	/** CamelContext */
	private CamelContext _context;
	/** Consumer instance for receive message */
	private PollingConsumer _consumer;

	/**
	 * Prepare for consuming data.
	 * 
	 * @param context
	 *            camel context for set end-point for consuming.
	 * @param uri
	 *            uri for consumer end-point. This prefix must set "direct:".
	 * @throws Exception
	 */
	void prepare(CamelContext context, String uri) throws Exception {
		// copy reference for context.
		_context = context;
		// get end-point from context.
		Endpoint ep = CamelContextHelper.getMandatoryEndpoint(_context, uri);
		// start end-point service.
		ServiceHelper.startService(ep);
		// create consumer instance from end-point
		_consumer = ep.createPollingConsumer();
		// add consumer as service to context.
		_context.addService(_consumer, true, true);
	}

	/**
	 * Consume data implementation.
	 * 
	 * @return received message.
	 */
	Message consume() {
		// Receive message as Camel Exchange.
		final Exchange exchange = _consumer.receive();
		// return Message in Exchange.
		return exchange.getIn();
	}
}
