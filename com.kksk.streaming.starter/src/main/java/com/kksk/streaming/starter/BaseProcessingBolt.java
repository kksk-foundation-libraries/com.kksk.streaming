package com.kksk.streaming.starter;

import java.util.Map;
import java.util.function.Function;

import org.apache.camel.CamelExecutionException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("rawtypes")
public abstract class BaseProcessingBolt<IN, OUT> extends BaseRichBolt {
	/** serialVersionUID */
	private static final long serialVersionUID = 4423818760322038174L;
	private static final Logger LOG = LoggerFactory.getLogger(BaseProcessingBolt.class);

	private OutputCollector _collector;

	private Function<IN, OUT> processor;
	private String inputFieldName;
	private String outputFieldName;

	protected abstract Function<IN, OUT> processor(Map conf);

	protected abstract String inputFieldName();

	protected String outputFieldName() {
		return null;
	}

	@Override
	public void prepare(Map conf, TopologyContext topologyContext, OutputCollector collector) {
		_collector = collector;
		processor = processor(conf);
		inputFieldName = inputFieldName();
		outputFieldName = outputFieldName();

		setUp(conf, topologyContext, collector);
	}

	protected void setUp(Map conf, TopologyContext topologyContext, OutputCollector collector) {
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple tuple) {
		try {
			IN input = (IN) tuple.getValueByField(inputFieldName);
			OUT output = processor.apply(input);
			_collector.ack(tuple);
			if (outputFieldName != null) {
				_collector.emit(tuple, new Values(output));
			}
		} catch (CamelExecutionException e) {
			LOG.error("unknown error.", e);
			_collector.fail(tuple);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		if (outputFieldName != null) {
			declarer.declare(new Fields(outputFieldName));
		}
	}
}
