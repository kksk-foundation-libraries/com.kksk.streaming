package com.kksk.streaming.starter;

import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.CamelExecutionException;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.RoutesBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("rawtypes")
public abstract class BaseCamelProducerBolt<IN, OUT> extends BaseRichBolt {
	/** serialVersionUID */
	private static final long serialVersionUID = 3086383458129072705L;
	private static final Logger LOG = LoggerFactory.getLogger(BaseCamelProducerBolt.class);
	protected static final String FROM_ENDPOINT_URI = "direct:entrance";
	private OutputCollector _collector;
	private CamelContext camelContext;
	private ProducerTemplate producerTemplate;
	private String inputFieldName;

	protected abstract RoutesBuilder routeBuilder(Map conf);

	protected abstract OUT transform(IN input);

	protected abstract String inputFieldName();

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		camelContext = new DefaultCamelContext();
		inputFieldName = inputFieldName();

		setUp(camelContext, stormConf, context, collector);
		try {
			camelContext.addRoutes(routeBuilder(stormConf));
			producerTemplate = camelContext.createProducerTemplate(1);
			camelContext.start();
		} catch (Exception e) {
			LOG.error("unknown error.", e);
			throw new RuntimeException(e);
		}
	}

	protected void setUp(CamelContext camelContext, Map stormConf, TopologyContext context, OutputCollector collector) {
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple tuple) {
		IN input = (IN) tuple.getValueByField(inputFieldName);

		try {
			producerTemplate.sendBody(FROM_ENDPOINT_URI, transform(input));
			_collector.ack(tuple);
		} catch (CamelExecutionException e) {
			LOG.error("unknown error.", e);
			_collector.fail(tuple);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void cleanup() {
		try {
			camelContext.stop();
		} catch (Exception e) {
			LOG.error("unknown error.", e);
			throw new RuntimeException(e);
		} finally {
			super.cleanup();
		}
	}
}
