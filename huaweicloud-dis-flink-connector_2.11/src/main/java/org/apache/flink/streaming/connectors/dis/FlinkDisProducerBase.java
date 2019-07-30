/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.dis;

import com.huaweicloud.dis.DISConfig;
import com.huaweicloud.dis.adapter.kafka.clients.producer.*;
import com.huaweicloud.dis.adapter.kafka.common.Metric;
import com.huaweicloud.dis.adapter.kafka.common.MetricName;
import com.huaweicloud.dis.adapter.kafka.common.PartitionInfo;
import com.huaweicloud.dis.adapter.kafka.common.serialization.ByteArraySerializer;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.dis.internals.metrics.DISMetricWrapper;
import org.apache.flink.streaming.connectors.dis.partitioner.FlinkDisPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.util.SerializableObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static java.util.Objects.requireNonNull;

/**
 * Flink Sink to produce data into a DIS stream.
 *
 * <p>Please note that this producer provides at-least-once reliability guarantees when
 * checkpoints are enabled and setFlushOnCheckpoint(true) is set.
 * Otherwise, the producer doesn't provide any reliability guarantees.
 *
 * @param <IN> Type of the messages to write into DIS.
 */
@Internal
public abstract class FlinkDisProducerBase<IN> extends RichSinkFunction<IN> implements CheckpointedFunction {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkDisProducerBase.class);

	private static final long serialVersionUID = 1L;

	/**
	 * Configuration key for disabling the metrics reporting.
	 */
	public static final String KEY_DISABLE_METRICS = "flink.disable-metrics";

	/**
	 * User defined properties for the Producer.
	 */
	protected final DISConfig disConfig;

	/**
	 * The name of the default Stream this producer is writing data to.
	 */
	protected final String defaultStreamName;

	/**
	 * (Serializable) SerializationSchema for turning objects used with Flink into.
	 * byte[] for DIS.
	 */
	protected final KeyedSerializationSchema<IN> schema;

	/**
	 * User-provided partitioner for assigning an object to a DIS partition for each stream.
	 */
	protected final FlinkDisPartitioner<IN> flinkDisPartitioner;

	/**
	 * Partitions of each topic.
	 */
	protected final Map<String, int[]> topicPartitionsMap;

	/**
	 * Flag indicating whether to accept failures (and log them), or to fail on failures.
	 */
	protected boolean logFailuresOnly;

	/**
	 * If true, the producer will wait until all outstanding records have been send to the broker.
	 */
	protected boolean flushOnCheckpoint = true;

	// -------------------------------- Runtime fields ------------------------------------------

	/** DISKafkaProducer instance. */
	protected transient DISKafkaProducer<byte[], byte[]> producer;

	/** The callback than handles error propagation or logging callbacks. */
	protected transient Callback callback;

	/** Errors encountered in the async producer are stored here. */
	protected transient volatile Exception asyncException;

	/** Lock for accessing the pending records. */
	protected final SerializableObject pendingRecordsLock = new SerializableObject();

	/** Number of unacknowledged records. */
	protected long pendingRecords;

	/**
	 * The main constructor for creating a FlinkDisProducer.
	 *
	 * @param defaultStreamName The default stream to write data to
	 * @param serializationSchema A serializable serialization schema for turning user objects into a DIS-consumable byte[] supporting key/value messages
	 * @param disConfig Configuration properties for the DISKafkaProducer.
	 * @param customPartitioner A serializable partitioner for assigning messages to DIS partitions. Passing null will use DIS's partitioner.
	 */
	public FlinkDisProducerBase(String defaultStreamName, KeyedSerializationSchema<IN> serializationSchema, DISConfig disConfig, FlinkDisPartitioner<IN> customPartitioner) {
		requireNonNull(defaultStreamName, "StreamName not set");
		requireNonNull(serializationSchema, "serializationSchema not set");
		requireNonNull(disConfig, "disConfig not set");
		ClosureCleaner.clean(customPartitioner, true);
		ClosureCleaner.ensureSerializable(serializationSchema);

		this.defaultStreamName = defaultStreamName;
		this.schema = serializationSchema;
		this.disConfig = disConfig;
		this.flinkDisPartitioner = customPartitioner;

		// set the producer configuration properties for DIS record key value serializers.
		if (!disConfig.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)) {
			this.disConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		} else {
			LOG.warn("Overwriting the '{}' is not recommended", ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
		}

		if (!disConfig.containsKey(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)) {
			this.disConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		} else {
			LOG.warn("Overwriting the '{}' is not recommended", ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
		}

		this.topicPartitionsMap = new HashMap<>();
	}

	// ---------------------------------- Properties --------------------------

	/**
	 * Defines whether the producer should fail on errors, or only log them.
	 * If this is set to true, then exceptions will be only logged, if set to false,
	 * exceptions will be eventually thrown and cause the streaming program to
	 * fail (and enter recovery).
	 *
	 * @param logFailuresOnly The flag to indicate logging-only on exceptions.
	 */
	public void setLogFailuresOnly(boolean logFailuresOnly) {
		this.logFailuresOnly = logFailuresOnly;
	}

	/**
	 * If set to true, the Flink producer will wait for all outstanding messages in the DIS buffers
	 * to be acknowledged by the DIS producer on a checkpoint.
	 * This way, the producer can guarantee that messages in the DIS buffers are part of the checkpoint.
	 *
	 * @param flush Flag indicating the flushing mode (true = flush on checkpoint)
	 */
	public void setFlushOnCheckpoint(boolean flush) {
		this.flushOnCheckpoint = flush;
	}

	/**
	 * Used for testing only.
	 */
	@VisibleForTesting
	protected <K, V> DISKafkaProducer<K, V> getDISKafkaProducer(DISConfig disConfig) {
		return new DISKafkaProducer<>(disConfig);
	}

	// ----------------------------------- Utilities --------------------------

	/**
	 * Initializes the connection to DIS.
	 */
	@Override
	public void open(Configuration configuration) {
		producer = getDISKafkaProducer(this.disConfig);

		RuntimeContext ctx = getRuntimeContext();

		if (null != flinkDisPartitioner) {
			flinkDisPartitioner.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());
		}

		LOG.info("Starting FlinkDisProducer ({}/{}) to produce into default topic {}",
				ctx.getIndexOfThisSubtask() + 1, ctx.getNumberOfParallelSubtasks(), defaultStreamName);

		// register DIS metrics to Flink accumulators
		if (!Boolean.parseBoolean(disConfig.getProperty(KEY_DISABLE_METRICS, "false"))) {
			Map<MetricName, ? extends Metric> metrics = this.producer.metrics();

			if (metrics == null) {
				// MapR's DIS implementation returns null here.
				LOG.info("Producer implementation does not support metrics");
			} else {
				final MetricGroup disMetricGroup = getRuntimeContext().getMetricGroup().addGroup("DISKafkaProducer");
				for (Map.Entry<MetricName, ? extends Metric> metric: metrics.entrySet()) {
					disMetricGroup.gauge(metric.getKey().name(), new DISMetricWrapper(metric.getValue()));
				}
			}
		}

		if (flushOnCheckpoint && !((StreamingRuntimeContext) this.getRuntimeContext()).isCheckpointingEnabled()) {
			LOG.warn("Flushing on checkpoint is enabled, but checkpointing is not enabled. Disabling flushing.");
			flushOnCheckpoint = false;
		}

		if (logFailuresOnly) {
			callback = new Callback() {
				@Override
				public void onCompletion(RecordMetadata metadata, Exception e) {
					if (e != null) {
						LOG.error("Error while sending record to DIS: " + e.getMessage(), e);
					}
					acknowledgeMessage();
				}
			};
		}
		else {
			callback = new Callback() {
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if (exception != null && asyncException == null) {
						asyncException = exception;
					}
					acknowledgeMessage();
				}
			};
		}
	}

	/**
	 * Called when new data arrives to the sink, and forwards it to DIS.
	 *
	 * @param next
	 * 		The incoming data
	 */
	@Override
	public void invoke(IN next, Context context) throws Exception {
		// propagate asynchronous errors
		checkErroneous();

		byte[] serializedKey = schema.serializeKey(next);
		byte[] serializedValue = schema.serializeValue(next);
		String targetTopic = schema.getTargetTopic(next);
		if (targetTopic == null) {
			targetTopic = defaultStreamName;
		}

		int[] partitions = this.topicPartitionsMap.get(targetTopic);
		if (null == partitions) {
			partitions = getPartitionsByTopic(targetTopic, producer);
			this.topicPartitionsMap.put(targetTopic, partitions);
		}

		ProducerRecord<byte[], byte[]> record;
		if (flinkDisPartitioner == null) {
			record = new ProducerRecord<>(targetTopic, serializedKey, serializedValue);
		} else {
			record = new ProducerRecord<>(
					targetTopic,
					flinkDisPartitioner.partition(next, serializedKey, serializedValue, targetTopic, partitions),
					serializedKey,
					serializedValue);
		}
		if (flushOnCheckpoint) {
			synchronized (pendingRecordsLock) {
				pendingRecords++;
			}
		}
		producer.send(record, callback);
	}

	@Override
	public void close() throws Exception {
		if (producer != null) {
			producer.close();
		}

		// make sure we propagate pending errors
		checkErroneous();
	}

	// ------------------- Logic for handling checkpoint flushing -------------------------- //

	private void acknowledgeMessage() {
		if (flushOnCheckpoint) {
			synchronized (pendingRecordsLock) {
				pendingRecords--;
				if (pendingRecords == 0) {
					pendingRecordsLock.notifyAll();
				}
			}
		}
	}

	/**
	 * Flush pending records.
	 */
	protected abstract void flush();

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		// nothing to do
	}

	@Override
	public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
		// check for asynchronous errors and fail the checkpoint if necessary
		checkErroneous();

		if (flushOnCheckpoint) {
			// flushing is activated: We need to wait until pendingRecords is 0
			flush();
			synchronized (pendingRecordsLock) {
				if (pendingRecords != 0) {
					throw new IllegalStateException("Pending record count must be zero at this point: " + pendingRecords);
				}

				// if the flushed requests has errors, we should propagate it also and fail the checkpoint
				checkErroneous();
			}
		}
	}

	// ----------------------------------- Utilities --------------------------

	protected void checkErroneous() throws Exception {
		Exception e = asyncException;
		if (e != null) {
			// prevent double throwing
			asyncException = null;
			throw new Exception("Failed to send data to DIS: " + e.getMessage(), e);
		}
	}

	protected static int[] getPartitionsByTopic(String topic, DISKafkaProducer<byte[], byte[]> producer) {
		// the fetched list is immutable, so we're creating a mutable copy in order to sort it
		List<PartitionInfo> partitionsList = new ArrayList<>(producer.partitionsFor(topic));

		// sort the partitions by partition id to make sure the fetched partition list is the same across subtasks
		Collections.sort(partitionsList, new Comparator<PartitionInfo>() {
			@Override
			public int compare(PartitionInfo o1, PartitionInfo o2) {
				return Integer.compare(o1.partition(), o2.partition());
			}
		});

		int[] partitions = new int[partitionsList.size()];
		for (int i = 0; i < partitions.length; i++) {
			partitions[i] = partitionsList.get(i).partition();
		}

		return partitions;
	}

	@VisibleForTesting
	protected long numPendingRecords() {
		synchronized (pendingRecordsLock) {
			return pendingRecords;
		}
	}
}