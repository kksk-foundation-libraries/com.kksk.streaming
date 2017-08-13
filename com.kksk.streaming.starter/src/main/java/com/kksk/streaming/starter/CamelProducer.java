package com.kksk.streaming.starter;

import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.processor.UnitOfWorkProducer;
import org.apache.camel.util.CamelContextHelper;
import org.apache.camel.util.ServiceHelper;

/**
 * camel producer utility
 */
class CamelProducer {
	/** camel context */
	private CamelContext _context;
	/** Producer instance for sending message. */
	private UnitOfWorkProducer _producer;

	/**
	 * Prepare for producing data.
	 * 
	 * @param context
	 *            camel context for set end-point for producing.
	 * @param uri
	 *            uri for producer end-point. This prefix must set "direct:".
	 * @throws Exception
	 */
	void prepare(CamelContext context, String uri) throws Exception {
		// copy reference for context.
		_context = context;
		// get end-point from context.
		Endpoint ep = CamelContextHelper.getMandatoryEndpoint(_context, uri);
		// start end-point service.
		ServiceHelper.startService(ep);
		// create producer instance from end-point
		_producer = new UnitOfWorkProducer(ep.createProducer());
		// add producer as service to context.
		_context.addService(_producer, true, true);
	}

	/**
	 * Send message to camel context.
	 * 
	 * @param message
	 *            message for processing on camel context.
	 * @throws Exception
	 */
	void produce(Message message) throws Exception {
		// ignore null message.
		if (message != null) {
			// create place holder of message.
			Exchange exchange = _producer.createExchange();
			// ignore null message header.
			if (message.getHeaders() != null)
				// copy message header.
				exchange.getIn().setHeaders(message.getHeaders());
			// copy message body.
			exchange.getIn().setBody(message.getBody());
			// ignore null message attachements
			if (message.getAttachments() != null)
				// copy message attachments.
				exchange.getIn().setAttachments(message.getAttachments());
			// send message which wrapped "Exchange"
			_producer.process(exchange);
		}
	}
}
