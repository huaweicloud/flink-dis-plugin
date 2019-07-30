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
import com.huaweicloud.dis.adapter.common.Utils;
import com.huaweicloud.dis.adapter.common.consumer.DisConsumerConfig;
import com.huaweicloud.dis.adapter.kafka.clients.consumer.Consumer;
import com.huaweicloud.dis.adapter.kafka.clients.consumer.DISKafkaConsumer;
import com.huaweicloud.dis.adapter.kafka.clients.consumer.OffsetAndTimestamp;
import com.huaweicloud.dis.adapter.kafka.common.TopicPartition;
import com.huaweicloud.dis.adapter.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.dis.config.OffsetCommitMode;
import org.apache.flink.streaming.connectors.dis.internals.*;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchemaWrapper;
import org.apache.flink.util.PropertiesUtil;
import org.apache.flink.util.SerializedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.PropertiesUtil.getBoolean;
import static org.apache.flink.util.PropertiesUtil.getLong;

/**
 * The Flink Dis Consumer is a streaming data source that pulls a parallel data stream from DIS.
 * The consumer can run in multiple parallel instances, each of which will pull data from one
 * or more DIS partitions.
 *
 * <p>The Flink DIS Consumer participates in checkpointing and guarantees that no data is lost
 * during a failure, and that the computation processes elements "exactly once".
 * (Note: These guarantees naturally assume that DIS itself does not loose any data.)</p>
 *
 */
@PublicEvolving
public class FlinkDisConsumer<T> extends FlinkDisConsumerBase<T>
{

	private static final long serialVersionUID = 2324564345203409112L;

    private static final Logger LOG = LoggerFactory.getLogger(FlinkDisConsumer.class);

    /**  Configuration key to change the polling timeout. **/
    public static final String KEY_POLL_TIMEOUT = "flink.poll-timeout";

    /** From Kafka's Javadoc: The time, in milliseconds, spent waiting in poll if data is not
     * available. If 0, returns immediately with any records that are available now. */
    public static final long DEFAULT_POLL_TIMEOUT = 2000L;

    // ------------------------------------------------------------------------

    /** User-supplied properties for DIS. **/
    protected final Properties properties;

    /** From Kafka's Javadoc: The time, in milliseconds, spent waiting in poll if data is not
     * available. If 0, returns immediately with any records that are available now */
    protected final long pollTimeout;
    
	// ------------------------------------------------------------------------


	public FlinkDisConsumer(String stream, DeserializationSchema<T> valueDeserializer, Properties props) {
		this(stream, valueDeserializer, Utils.newDisConfig(props));
	}

	public FlinkDisConsumer(String stream, DeserializationSchema<T> valueDeserializer, DISConfig disConfig) {
		this(Collections.singletonList(stream), valueDeserializer, disConfig);
	}

	/**
	 * Creates a new DIS streaming source consumer for DIS
	 *
	 * <p>This constructor allows passing a {@see KeyedDeserializationSchema} for reading key/value
	 * pairs, offsets, and topic names from DIS.
	 *
	 * @param stream
	 *           The name of the stream that should be consumed.
	 * @param deserializer
	 *           The keyed de-/serializer used to convert between DIS's byte messages and Flink's objects.
	 * @param props
	 *           The properties used to configure the DIS consumer client.
	 */
	public FlinkDisConsumer(String stream, KeyedDeserializationSchema<T> deserializer, Properties props) {
		this(stream, deserializer, Utils.newDisConfig(props));
	}

	/**
	 * Creates a new DIS streaming source consumer for DIS
	 *
	 * <p>This constructor allows passing a {@see KeyedDeserializationSchema} for reading key/value
	 * pairs, offsets, and topic names from DIS.
	 *
	 * @param stream
	 *           The name of the stream that should be consumed.
	 * @param deserializer
	 *           The keyed de-/serializer used to convert between DIS's byte messages and Flink's objects.
	 * @param disConfig
	 *           The properties used to configure the DIS consumer client.
	 */
	public FlinkDisConsumer(String stream, KeyedDeserializationSchema<T> deserializer, DISConfig disConfig) {
		this(Collections.singletonList(stream), deserializer, disConfig);
	}

	/**
	 * Creates a new DIS streaming source consumer for DIS
	 *
	 * <p>This constructor allows passing multiple topics to the consumer.
	 *
	 * @param streams
	 *           The DIS streams to read from.
	 * @param deserializer
	 *           The de-/serializer used to convert between DIS's byte messages and Flink's objects.
	 * @param props
	 *           The properties that are used to configure both the fetcher and the offset handler.
	 */
	public FlinkDisConsumer(List<String> streams, DeserializationSchema<T> deserializer, Properties props) {
		this(streams, deserializer, Utils.newDisConfig(props));
	}

	/**
	 * Creates a new DIS streaming source consumer for DIS
	 *
	 * <p>This constructor allows passing multiple topics to the consumer.
	 *
	 * @param streams
	 *           The DIS streams to read from.
	 * @param deserializer
	 *           The de-/serializer used to convert between DIS's byte messages and Flink's objects.
	 * @param disConfig
	 *           The properties that are used to configure both the fetcher and the offset handler.
	 */
	public FlinkDisConsumer(List<String> streams, DeserializationSchema<T> deserializer, DISConfig disConfig) {
		this(streams, new KeyedDeserializationSchemaWrapper<>(deserializer), disConfig);
	}

	/**
	 * Creates a new DIS streaming source consumer for DIS
	 *
	 * <p>This constructor allows passing multiple topics and a key/value deserialization schema.
	 *
	 * @param streams
	 *           The DIS streams to read from.
	 * @param deserializer
	 *           The keyed de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties that are used to configure both the fetcher and the offset handler.
	 */
	public FlinkDisConsumer(List<String> streams, KeyedDeserializationSchema<T> deserializer, Properties props) {
		this(streams, deserializer, Utils.newDisConfig(props));
	}

	/**
	 * Creates a new DIS streaming source consumer for DIS
	 *
	 * <p>This constructor allows passing multiple topics and a key/value deserialization schema.
	 *
	 * @param streams
	 *           The DIS streams to read from.
	 * @param deserializer
	 *           The keyed de-/serializer used to convert between DIS's byte messages and Flink's objects.
	 * @param disConfig
	 *           The properties that are used to configure both the fetcher and the offset handler.
	 */
	public FlinkDisConsumer(List<String> streams, KeyedDeserializationSchema<T> deserializer, DISConfig disConfig) {
		this(streams, null, deserializer, disConfig);
	}

	/**
	 * Creates a new DIS streaming source consumer for DIS. Use this constructor to
	 * subscribe to multiple streams based on a regular expression pattern.
	 *
	 * <p>If partition discovery is enabled (by setting a non-negative value for
	 * {@link FlinkDisConsumer#KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS} in the properties), streams
	 * with names matching the pattern will also be subscribed to as they are created on the fly.
	 *
	 * @param subscriptionPattern
	 *           The regular expression for a pattern of topic names to subscribe to.
	 * @param valueDeserializer
	 *           The de-/serializer used to convert between DIS's byte messages and Flink's objects.
	 * @param props
	 *           The properties used to configure the DIS consumer client.
	 */
	@PublicEvolving
	public FlinkDisConsumer(Pattern subscriptionPattern, DeserializationSchema<T> valueDeserializer, Properties props) {
		this(subscriptionPattern, valueDeserializer, Utils.newDisConfig(props));
	}

	/**
	 * Creates a new DIS streaming source consumer for DIS. Use this constructor to
	 * subscribe to multiple streams based on a regular expression pattern.
	 *
	 * <p>If partition discovery is enabled (by setting a non-negative value for
	 * {@link FlinkDisConsumer#KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS} in the properties), streams
	 * with names matching the pattern will also be subscribed to as they are created on the fly.
	 *
	 * @param subscriptionPattern
	 *           The regular expression for a pattern of topic names to subscribe to.
	 * @param valueDeserializer
	 *           The de-/serializer used to convert between DIS's byte messages and Flink's objects.
	 * @param disConfig
	 *           The properties used to configure the DIS consumer client.
	 */
	@PublicEvolving
	public FlinkDisConsumer(Pattern subscriptionPattern, DeserializationSchema<T> valueDeserializer, DISConfig disConfig) {
		this(subscriptionPattern, new KeyedDeserializationSchemaWrapper<>(valueDeserializer), disConfig);
	}

	/**
	 * Creates a new DIS streaming source consumer for DIS. Use this constructor to
	 * subscribe to multiple streams based on a regular expression pattern.
	 *
	 * <p>If partition discovery is enabled (by setting a non-negative value for
	 * {@link FlinkDisConsumer#KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS} in the properties), topics
	 * with names matching the pattern will also be subscribed to as they are created on the fly.
	 *
	 * <p>This constructor allows passing a {@see KeyedDeserializationSchema} for reading key/value
	 * pairs, offsets, and stream names from DIS.
	 *
	 * @param subscriptionPattern
	 *           The regular expression for a pattern of topic names to subscribe to.
	 * @param deserializer
	 *           The keyed de-/serializer used to convert between DIS's byte messages and Flink's objects.
	 * @param props
	 *           The properties used to configure the DIS consumer client.
	 */
	@PublicEvolving
	public FlinkDisConsumer(Pattern subscriptionPattern, KeyedDeserializationSchema<T> deserializer, Properties props) {
		this(subscriptionPattern, deserializer, Utils.newDisConfig(props));
	}

	/**
	 * Creates a new DIS streaming source consumer for DIS. Use this constructor to
	 * subscribe to multiple streams based on a regular expression pattern.
	 *
	 * <p>If partition discovery is enabled (by setting a non-negative value for
	 * {@link FlinkDisConsumer#KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS} in the properties), topics
	 * with names matching the pattern will also be subscribed to as they are created on the fly.
	 *
	 * <p>This constructor allows passing a {@see KeyedDeserializationSchema} for reading key/value
	 * pairs, offsets, and stream names from DIS.
	 *
	 * @param subscriptionPattern
	 *           The regular expression for a pattern of topic names to subscribe to.
	 * @param deserializer
	 *           The keyed de-/serializer used to convert between DIS's byte messages and Flink's objects.
	 * @param disConfig
	 *           The properties used to configure the DIS consumer client.
	 */
	@PublicEvolving
	public FlinkDisConsumer(Pattern subscriptionPattern, KeyedDeserializationSchema<T> deserializer, DISConfig disConfig) {
		this(null, subscriptionPattern, deserializer, disConfig);
	}

    private FlinkDisConsumer(
        List<String> streams,
        Pattern subscriptionPattern,
        KeyedDeserializationSchema<T> deserializer,
        DISConfig disConfig) {

        super(
			streams,
            subscriptionPattern,
            deserializer,
            getLong(
                checkNotNull(disConfig, "props"),
                KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, PARTITION_DISCOVERY_DISABLED),
            !getBoolean(disConfig, KEY_DISABLE_METRICS, false));

        this.properties = disConfig;
        setDeserializer(this.properties);

        // configure the polling timeout
        try {
            if (properties.containsKey(KEY_POLL_TIMEOUT)) {
                this.pollTimeout = Long.parseLong(properties.getProperty(KEY_POLL_TIMEOUT));
            } else {
                this.pollTimeout = DEFAULT_POLL_TIMEOUT;
            }
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Cannot parse poll timeout for '" + KEY_POLL_TIMEOUT + '\'', e);
        }
    }
    
	@Override
	protected AbstractFetcher<T, ?> createFetcher(
			SourceFunction.SourceContext<T> sourceContext,
			Map<DisStreamPartition, Long> assignedPartitionsWithInitialOffsets,
			SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
			SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
			StreamingRuntimeContext runtimeContext,
			OffsetCommitMode offsetCommitMode,
			MetricGroup consumerMetricGroup,
			boolean useMetrics) throws Exception {

		// make sure that auto commit is disabled when our offset commit mode is ON_CHECKPOINTS;
		// this overwrites whatever setting the user configured in the properties
		if (offsetCommitMode == OffsetCommitMode.ON_CHECKPOINTS || offsetCommitMode == OffsetCommitMode.DISABLED) {
			properties.setProperty(DisConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		}

		return new DisFetcher<>(
				sourceContext,
				assignedPartitionsWithInitialOffsets,
				watermarksPeriodic,
				watermarksPunctuated,
				runtimeContext.getProcessingTimeService(),
				runtimeContext.getExecutionConfig().getAutoWatermarkInterval(),
				runtimeContext.getUserCodeClassLoader(),
				runtimeContext.getTaskNameWithSubtasks(),
				deserializer,
				properties,
				pollTimeout,
				runtimeContext.getMetricGroup(),
				consumerMetricGroup,
				useMetrics);
	}

	@Override
	protected AbstractPartitionDiscoverer createPartitionDiscoverer(
			DisStreamsDescriptor topicsDescriptor,
			int indexOfThisSubtask,
			int numParallelSubtasks) {

		return new DisPartitionDiscoverer(topicsDescriptor, indexOfThisSubtask, numParallelSubtasks, properties);
	}

	// ------------------------------------------------------------------------
	//  Timestamp-based startup
	// ------------------------------------------------------------------------

	@Override
	public FlinkDisConsumerBase<T> setStartFromTimestamp(long startupOffsetsTimestamp) {
		// the purpose of this override is just to publicly expose the method for Kafka 0.10+;
		// the base class doesn't publicly expose it since not all Kafka versions support the functionality
		return super.setStartFromTimestamp(startupOffsetsTimestamp);
	}

	@Override
	protected Map<DisStreamPartition, Long> fetchOffsetsWithTimestamp(
			Collection<DisStreamPartition> partitions,
			long timestamp) {

		Map<TopicPartition, Long> partitionOffsetsRequest = new HashMap<>(partitions.size());
		for (DisStreamPartition partition : partitions) {
			partitionOffsetsRequest.put(
				new TopicPartition(partition.getTopic(), partition.getPartition()),
				timestamp);
		}

		// use a short-lived consumer to fetch the offsets;
		// this is ok because this is a one-time operation that happens only on startup
		Consumer<?, ?> consumer = new DISKafkaConsumer<>(properties);

		Map<DisStreamPartition, Long> result = new HashMap<>(partitions.size());
        
		for (Map.Entry<TopicPartition, OffsetAndTimestamp> partitionToOffset :
				consumer.offsetsForTimes(partitionOffsetsRequest).entrySet()) {

			result.put(
				new DisStreamPartition(partitionToOffset.getKey().topic(), partitionToOffset.getKey().partition()),
				(partitionToOffset.getValue() == null) ? null : partitionToOffset.getValue().offset());
		}
		

		consumer.close();
		return result;
	}
	
    @Override
    protected boolean getIsAutoCommitEnabled() {
        return getBoolean(properties, DisConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true) &&
            PropertiesUtil.getLong(properties, DisConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000) > 0;
    }

    /**
     * Makes sure that the ByteArrayDeserializer is registered in the DIS properties.
     *
     * @param props The DIS properties to register the serializer in.
     */
    private static void setDeserializer(Properties props) {
        final String deSerName = ByteArrayDeserializer.class.getName();

        Object keyDeSer = props.get(DisConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        Object valDeSer = props.get(DisConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);

        if (keyDeSer != null && !keyDeSer.equals(deSerName)) {
            LOG.warn("Ignoring configured key DeSerializer ({})", DisConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        }
        if (valDeSer != null && !valDeSer.equals(deSerName)) {
            LOG.warn("Ignoring configured value DeSerializer ({})", DisConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
        }

        props.put(DisConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deSerName);
        props.put(DisConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deSerName);
    }
}
