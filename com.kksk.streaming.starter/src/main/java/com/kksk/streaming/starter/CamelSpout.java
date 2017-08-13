package com.kksk.streaming.starter;

import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.Message;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spout using camel consuming message function
 */
@SuppressWarnings("rawtypes")
public final class CamelSpout extends BaseRichSpout {
	/** serialVersionUID */
	private static final long serialVersionUID = -8617520279593091693L;
	private static final Logger LOG = LoggerFactory.getLogger(CamelSpout.class);

	/** message stream */
	private SpoutOutputCollector _collector;
	/** camel context */
	private CamelContext _context;
	/** camel consumer utility */
	private CamelConsumer _consumer;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// copy reference.
		_collector = collector;
		// create new default context
		_context = new DefaultCamelContext();
		// create new camel consumer utility instance
		_consumer = new CamelConsumer();
		try {
			// set camel consumer utility up.
			_consumer.prepare(_context, (String) conf.get("from:uri"));
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
	public void nextTuple() {
		// poll message.
		Message msg = _consumer.consume();
		// and fire stream event.
		_collector.emit(new Values(msg));
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
	public void close() {
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
			super.close();
		}
	}
}
