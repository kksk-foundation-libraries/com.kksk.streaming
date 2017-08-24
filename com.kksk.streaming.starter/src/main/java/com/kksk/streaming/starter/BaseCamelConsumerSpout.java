package com.kksk.streaming.starter;

import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.ConsumerTemplate;
import org.apache.camel.RoutesBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("rawtypes")
public abstract class BaseCamelConsumerSpout<IN, OUT> extends BaseRichSpout {
	/** serialVersionUID */
	private static final long serialVersionUID = 6433348096297141184L;

	private static final Logger LOG = LoggerFactory.getLogger(BaseCamelConsumerSpout.class);

	protected static final String TO_ENDPOINT_URI = "direct:exit";
	private SpoutOutputCollector _collector;
	private CamelContext camelContext;
	private ConsumerTemplate consumerTemplate;

	protected abstract RoutesBuilder routeBuilder(Map conf);

	protected abstract OUT transform(IN input);

	protected abstract String outputFieldName();

	@Override
	public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector collector) {
		_collector = collector;
		camelContext = new DefaultCamelContext();
		setUp(camelContext, conf, topologyContext, collector);
		try {
			camelContext.addRoutes(routeBuilder(conf));
			consumerTemplate = camelContext.createConsumerTemplate(1);
			camelContext.start();
		} catch (Exception e) {
			LOG.error("unknown error.", e);
			throw new RuntimeException(e);
		}
	}

	protected void setUp(CamelContext camelContext, Map conf, TopologyContext topologyContext, SpoutOutputCollector collector) {
	}

	@SuppressWarnings("unchecked")
	@Override
	public final void nextTuple() {
		IN input = (IN) consumerTemplate.receiveBody(TO_ENDPOINT_URI);
		_collector.emit(new Values(transform(input)));
	}

	@Override
	public final void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(outputFieldName()));
	}

	@Override
	public void close() {
		try {
			camelContext.stop();
		} catch (Exception e) {
			LOG.error("unknown error.", e);
			throw new RuntimeException(e);
		} finally {
			super.close();
		}
	}
}
