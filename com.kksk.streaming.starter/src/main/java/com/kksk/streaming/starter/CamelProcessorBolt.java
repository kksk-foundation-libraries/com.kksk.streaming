package com.kksk.streaming.starter;

import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.Message;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
@SuppressWarnings("rawtypes")
public class CamelProcessorBolt extends BaseRichBolt {
	/** serialVersionUID */
	private static final long serialVersionUID = -8438433026551858792L;
	private static final Logger LOG = LoggerFactory.getLogger(CamelProcessorBolt.class);

	/** message stream */
	private OutputCollector _collector;
	/** camel context */
	private CamelContext _context;
	/** camel producer utility */
	private CamelProducer _producer;
	/** camel consumer utility */
	private CamelConsumer _consumer;
	/** use output */
	private boolean hasOutUri;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// copy reference.
		_collector = collector;
		// create new default context
		_context = new DefaultCamelContext();
		// create new camel producer utility instance
		_producer = new CamelProducer();
		// create new camel consumer utility instance
		_consumer = new CamelConsumer();
		try {
			hasOutUri = stormConf.get("out:uri") != null;
			if (hasOutUri) {
				// set camel consumer utility up.
				_consumer.prepare(_context, (String) stormConf.get("out:uri"));
			}

			// set camel route builder up.
			String className = (String) stormConf.get("routeBuilder:className");
			// using reflection function, because cannot receive object
			// instance.
			RouteBuilder routeBuilder = (RouteBuilder) Class.forName(className).newInstance();
			// add route builder instance to context.
			_context.addRoutes(routeBuilder);

			// set camel consumer utility up.
			_producer.prepare(_context, (String) stormConf.get("in:uri"));
			// start camel context
			_context.start();
		} catch (Exception e) {
			// Don't worry about what's happened.
			LOG.error("unkonwn error.", e);
			// throw throw throw...
			throw new RuntimeException(e);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void execute(Tuple input) {
		try {
			// poll message from storm stream, and produce to camel route.
			_producer.produce((Message) input.getValueByField("message"));
			// ack to storm stream.
			_collector.ack(input);
			if (hasOutUri) {
				// poll message from camel route.
				Message message = _consumer.consume();
				// produce message to storm stream.
				_collector.emit(new Values(message));
			}
		} catch (Exception e) {
			// Don't worry about what's happened.
			LOG.error("unkonwn error.", e);
			// append fail message to storm stream.
			_collector.fail(input);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// "message" field only.
		declarer.declare(new Fields("message"));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void cleanup() {
		try {
			// at first, stop camel context.
			_context.stop();
		} catch (Exception e) {
			// Don't worry about what's happened.
			LOG.error("unkonwn error.", e);
			// throw throw throw...
			throw new RuntimeException(e);
		} finally {
			// must stop parent.
			super.cleanup();
		}
	}
}
