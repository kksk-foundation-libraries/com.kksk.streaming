package com.kksk.streaming.starter;

import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.Message;
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

import rx.Observable;
import rx.subjects.PublishSubject;

/**
 * 
 */
@SuppressWarnings("rawtypes")
public abstract class BaseRxObservableBolt<T> extends BaseRichBolt {
	/** serialVersionUID */
	private static final long serialVersionUID = 2645106009853207583L;
	private static final Logger LOG = LoggerFactory.getLogger(BaseRxObservableBolt.class);

	/** message stream */
	private OutputCollector _collector;
	/** camel context */
	private CamelContext _context;
	/** camel consumer utility */
	private CamelConsumer _consumer;
	/** use output */
	private boolean hasOutUri;
	private PublishSubject<T> _subject;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		try {
			// copy reference.
			_collector = collector;

			hasOutUri = stormConf.get("out:uri") != null;
			if (hasOutUri) {
				// create new default context
				_context = new DefaultCamelContext();
				// create new camel consumer utility instance
				_consumer = new CamelConsumer();
				// set camel consumer utility up.
				_consumer.prepare(_context, (String) stormConf.get("out:uri"));
				// start camel context
				_context.start();
			} else {
				_context = null;
			}
			_subject = PublishSubject.create();
			prepareRx(_subject, _context, (String) stormConf.get("out:uri"));
		} catch (Exception e) {
			// Don't worry about what's happened.
			LOG.error("unkonwn error.", e);
			// throw throw throw...
			throw new RuntimeException(e);
		}
	}

	/**
	 * setting observers up by camel context and rx.observable.
	 * 
	 * @param observable
	 *            rx observable
	 * @param context
	 *            camel context(when "out:uri" is not defined by topology
	 *            builder, will be set null.)
	 * @param outUri
	 *            camel endpoint output uri of processor
	 * @throws Exception
	 */
	protected abstract void prepareRx(final Observable<T> observable, final CamelContext context, final String outUri) throws Exception;

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	@Override
	public final void execute(Tuple input) {
		try {
			Message message = (Message) input.getValueByField("message");
			_subject.onNext((T) message.getBody());
			if (hasOutUri) {
				message = _consumer.consume();
				_collector.emit(new Values(message));
			}
			_collector.ack(input);
		} catch (Exception e) {
			_subject.onError(e);
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
			_subject.onCompleted();
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
