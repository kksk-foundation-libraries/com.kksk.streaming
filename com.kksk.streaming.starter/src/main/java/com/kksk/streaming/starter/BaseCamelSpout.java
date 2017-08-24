package com.kksk.streaming.starter;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.camel.PollingConsumer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("rawtypes")
public abstract class BaseCamelSpout extends BaseRichSpout {
	/** serialVersionUID */
	private static final long serialVersionUID = 5518716807016100831L;
	private static final Logger LOG = LoggerFactory.getLogger(BaseCamelSpout.class);
	private SpoutOutputCollector _collector;
	private CamelContextSupport camelContextSupport;
	private ConcurrentMap<String, PollingConsumer> topologyConsumers;
	private ConcurrentMap<String, String> outputFieldByStreamId;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		camelContextSupport = new CamelContextSupport();
		topologyConsumers = new ConcurrentHashMap<>();
		Map<String, String> uris = consumerEndpointUrisByStreamId(conf);
		try {
			// PollingConsumer c = camelContextSupport.createDirectConsumer("");
			// c.getEndpoint().getEndpointUri();
			for (Entry<String, String> entry : uris.entrySet()) {
				topologyConsumers.putIfAbsent(entry.getKey(), camelContextSupport.createDirectConsumer(entry.getValue()));
			}
			outputFieldByStreamId = new ConcurrentHashMap<>(outputFieldByStreamId(conf));
			camelContextSupport.start();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	protected abstract Map<String, String> consumerEndpointUrisByStreamId(Map conf);

	protected abstract Map<String, String> outputFieldByStreamId(Map conf);

	@Override
	public final void nextTuple() {
		// _collector.emit(streamId, new Values());
	}

	@Override
	public final void declareOutputFields(OutputFieldsDeclarer declarer) {
		for (Entry<String, String> entry : outputFieldByStreamId.entrySet()) {
			declarer.declareStream(entry.getKey(), new Fields(entry.getValue()));
		}
	}

	protected final CamelContextSupport camelContextSupport() {
		return camelContextSupport;
	}

	@Override
	public void close() {
		try {
			camelContextSupport.stop();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			super.close();
		}
	}
}
