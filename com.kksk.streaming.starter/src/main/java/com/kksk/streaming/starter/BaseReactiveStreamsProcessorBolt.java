package com.kksk.streaming.starter;

import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.processor.UnitOfWorkProducer;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
@SuppressWarnings("rawtypes")
public abstract class BaseReactiveStreamsProcessorBolt<T, R> extends BaseRichBolt {
	/** serialVersionUID */
	private static final long serialVersionUID = 2645106009853207583L;
	private static final Logger LOG = LoggerFactory.getLogger(BaseReactiveStreamsProcessorBolt.class);

	/** message stream */
	private OutputCollector _collector;
	/** camel context */
	private CamelContext _context;
	/** camel consumer utility */
	private CamelConsumer _consumer;
	/** use output */
	private boolean hasOutUri;
	private UnitOfWorkProducer _producer;
	private Processor<T, R> _processor;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		try {
			// copy reference.
			_collector = collector;

			final String outUri = (String) stormConf.get("out:uri");
			hasOutUri = outUri != null;
			Subscriber<R> subscriber = null;
			if (hasOutUri) {
				// create new default context
				_context = new DefaultCamelContext();
				// create new camel consumer utility instance
				_consumer = new CamelConsumer();
				// set camel consumer utility up.
				_consumer.prepare(_context, outUri);
				_producer = new UnitOfWorkProducer(_context.getEndpoint(outUri).createProducer());
				// add producer as service to context.
				_context.addService(_producer, true, true);
				// start camel context
				_context.start();
				subscriber = new Subscriber<R>() {
					@Override
					public void onSubscribe(Subscription s) {
						s.request(1);
					}

					@Override
					public void onNext(R t) {
						Exchange exchange = _producer.createExchange();
						exchange.getIn().setBody(t);
						try {
							_producer.process(exchange);
						} catch (Exception e) {
							throw new RuntimeException(e);
						}
					}

					@Override
					public void onError(Throwable t) {
					}

					@Override
					public void onComplete() {
					}
				};
			} else {
				_context = null;
			}
			_processor = prepareProcessor(stormConf);
			if (hasOutUri) {
				_processor.subscribe(subscriber);
			}
		} catch (Exception e) {
			// Don't worry about what's happened.
			LOG.error("unkonwn error.", e);
			// throw throw throw...
			throw new RuntimeException(e);
		}
	}

	/**
	 * setting processor up by camel context.
	 * 
	 * @param context
	 *            camel context(when "out:uri" is not defined by topology
	 *            builder, will be set null.)
	 * @param outUri
	 *            camel endpoint output uri of processor
	 * @throws Exception
	 */
	protected abstract Processor<T, R> prepareProcessor(final Map stormConf) throws Exception;

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	@Override
	public final void execute(Tuple input) {
		try {
			Message message = (Message) input.getValueByField("message");
			_processor.onNext((T) message.getBody());
			if (hasOutUri) {
				message = _consumer.consume();
				_collector.emit(new Values(message));
			}
			_collector.ack(input);
		} catch (Exception e) {
			_processor.onError(e);
			_collector.fail(input);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final void declareOutputFields(OutputFieldsDeclarer declarer) {
		if (hasOutUri) {
			// "message" field only.
			declarer.declare(new Fields("message"));
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final void cleanup() {
		try {
			// at first, stop rx stream.
			_processor.onComplete();
			// at second, stop camel context.
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
