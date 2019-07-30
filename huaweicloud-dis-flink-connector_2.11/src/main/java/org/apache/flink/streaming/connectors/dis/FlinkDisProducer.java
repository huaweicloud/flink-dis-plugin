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
import com.huaweicloud.dis.adapter.kafka.clients.producer.ProducerRecord;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.dis.partitioner.FlinkDisPartitioner;
import org.apache.flink.streaming.connectors.dis.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * Flink Sink to produce data into a DIS stream.
 */
@PublicEvolving
public class FlinkDisProducer<T> extends FlinkDisProducerBase<T> {

	private static final long serialVersionUID = 1L;

	/**
	 * Flag controlling whether we are writing the Flink record's timestamp into DIS.
	 */
	private boolean writeTimestampToKafka = false;

	// ---------------------- Regular constructors------------------

	/**
	 * Creates a FlinkDisProducer for a given stream. the sink produces a DataStream to
	 * the stream.
	 *
	 * <p>Using this constructor, the default {@link FlinkFixedPartitioner} will be used as
	 * the partitioner. This default partitioner maps each sink subtask to a single DIS
	 * partition (i.e. all records received by a sink subtask will end up in the same
	 * DIS partition).
	 *
	 * <p>To use a custom partitioner, please use
	 * {@link #FlinkDisProducer(String, SerializationSchema, DISConfig, FlinkDisPartitioner)} instead.
	 *
	 * @param streamName
	 * 			StreamName of DIS.
	 * @param serializationSchema
	 * 			User defined key-less serialization schema.
	 * @param producerConfig
	 * 			Properties with the producer configuration.
	 */
	public FlinkDisProducer(String streamName, SerializationSchema<T> serializationSchema, Properties producerConfig) {
		this(streamName, new KeyedSerializationSchemaWrapper<>(serializationSchema), Utils.newDisConfig(producerConfig));
	}

	/**
	 * Creates a FlinkDisProducer for a given stream. the sink produces a DataStream to
	 * the stream.
	 *
	 * <p>Using this constructor, the default {@link FlinkFixedPartitioner} will be used as
	 * the partitioner. This default partitioner maps each sink subtask to a single DIS
	 * partition (i.e. all records received by a sink subtask will end up in the same
	 * DIS partition).
	 *
	 * <p>To use a custom partitioner, please use
	 * {@link #FlinkDisProducer(String, SerializationSchema, DISConfig, FlinkDisPartitioner)} instead.
	 *
	 * @param streamName
	 * 			StreamName of DIS.
	 * @param serializationSchema
	 * 			User defined key-less serialization schema.
	 * @param disConfig
	 * 			Properties with the producer configuration.
	 */
	public FlinkDisProducer(String streamName, SerializationSchema<T> serializationSchema, DISConfig disConfig) {
		this(streamName, new KeyedSerializationSchemaWrapper<>(serializationSchema), disConfig, new FlinkFixedPartitioner<T>());
	}

	/**
	 * Creates a FlinkDisProducer for a given stream. The sink produces its input to
	 * the stream. It accepts a key-less {@link SerializationSchema} and possibly a custom {@link FlinkDisPartitioner}.
	 *
	 * <p>Since a key-less {@link SerializationSchema} is used, all records sent to DIS will not have an
	 * attached key. Therefore, if a partitioner is also not provided, records will be distributed to DIS
	 * partitions in a round-robin fashion.
	 *
	 * @param streamName The DIS Stream to write data to
	 * @param serializationSchema A key-less serializable serialization schema for turning user objects into a DIS-consumable byte[]
	 * @param disConfig Configuration properties for the DISKafkaProducer.
	 * @param customPartitioner A serializable partitioner for assigning messages to DIS partitions.
	 *                          If set to {@code null}, records will be distributed to DIS partitions
	 *                          in a round-robin fashion.
	 */
	public FlinkDisProducer(
			String streamName,
			SerializationSchema<T> serializationSchema,
			DISConfig disConfig,
			@Nullable FlinkDisPartitioner<T> customPartitioner) {

		this(streamName, new KeyedSerializationSchemaWrapper<>(serializationSchema), disConfig, customPartitioner);
	}

	// ------------------- Key/Value serialization schema constructors ----------------------

	/**
	 * Creates a FlinkDisProducer for a given topic. The sink produces a DataStream to
	 * the stream.
	 *
	 * <p>Using this constructor, the default {@link FlinkFixedPartitioner} will be used as
	 * the partitioner. This default partitioner maps each sink subtask to a single DIS
	 * partition (i.e. all records received by a sink subtask will end up in the same
	 * DIS partition).
	 *
	 * <p>To use a custom partitioner, please use
	 * {@link #FlinkDisProducer(String, KeyedSerializationSchema, DISConfig, FlinkDisPartitioner)} instead.
	 *
	 * @param streamName
	 * 			StreamName of DIS.
	 * @param serializationSchema
	 * 			User defined serialization schema supporting key/value messages
	 * @param disConfig
	 * 			Properties with the producer configuration.
	 */
	public FlinkDisProducer(String streamName, KeyedSerializationSchema<T> serializationSchema, DISConfig disConfig) {
		this(streamName, serializationSchema, disConfig, new FlinkFixedPartitioner<T>());
	}

	/**
	 * Creates a FlinkDisProducer for a given stream. The sink produces its input to
	 * the stream. It accepts a keyed {@link KeyedSerializationSchema} and possibly a custom {@link FlinkDisPartitioner}.
	 *
	 * <p>If a partitioner is not provided, written records will be partitioned by the attached key of each
	 * record (as determined by {@link KeyedSerializationSchema#serializeKey(Object)}). If written records do not
	 * have a key (i.e., {@link KeyedSerializationSchema#serializeKey(Object)} returns {@code null}), they
	 * will be distributed to DIS partitions in a round-robin fashion.
	 *
	 * @param streamName The DIS Stream to write data to
	 * @param serializationSchema A serializable serialization schema for turning user objects into a DIS-consumable byte[] supporting key/value messages
	 * @param disConfig Configuration properties for the DISKafkaProducer.
	 * @param customPartitioner A serializable partitioner for assigning messages to DIS partitions.
	 *                          If set to {@code null}, records will be partitioned by the key of each record
	 *                          (determined by {@link KeyedSerializationSchema#serializeKey(Object)}). If the keys
	 *                          are {@code null}, then records will be distributed to DIS partitions in a
	 *                          round-robin fashion.
	 */
	public FlinkDisProducer(
			String streamName,
			KeyedSerializationSchema<T> serializationSchema,
			DISConfig disConfig,
			@Nullable FlinkDisPartitioner<T> customPartitioner) {

		super(streamName, serializationSchema, disConfig, customPartitioner);
	}

	// ------------------- User configuration ----------------------

	/**
	 * If set to true, Flink will write the (event time) timestamp attached to each record into DIS.
	 * Timestamps must be positive for DIS to accept them.
	 *
	 * @param writeTimestampToKafka Flag indicating if Flink's internal timestamps are written to Kafka.
	 */
	public void setWriteTimestampToKafka(boolean writeTimestampToKafka) {
		this.writeTimestampToKafka = writeTimestampToKafka;
	}

	// ----------------------------- Generic element processing  ---------------------------

	@Override
	public void invoke(T value, Context context) throws Exception {

		checkErroneous();

		byte[] serializedKey = schema.serializeKey(value);
		byte[] serializedValue = schema.serializeValue(value);
		String targetTopic = schema.getTargetTopic(value);
		if (targetTopic == null) {
			targetTopic = defaultStreamName;
		}

		Long timestamp = null;
		if (this.writeTimestampToKafka) {
			timestamp = context.timestamp();
		}

		ProducerRecord<byte[], byte[]> record;
		int[] partitions = topicPartitionsMap.get(targetTopic);
		if (null == partitions) {
			partitions = getPartitionsByTopic(targetTopic, producer);
			topicPartitionsMap.put(targetTopic, partitions);
		}
		if (flinkDisPartitioner == null) {
			record = new ProducerRecord<>(targetTopic, null, timestamp, serializedKey, serializedValue);
		} else {
			record = new ProducerRecord<>(targetTopic, flinkDisPartitioner.partition(value, serializedKey, serializedValue, targetTopic, partitions), timestamp, serializedKey, serializedValue);
		}
		if (flushOnCheckpoint) {
			synchronized (pendingRecordsLock) {
				pendingRecords++;
			}
		}
		producer.send(record, callback);
	}

	@Override
	protected void flush() {
		if (this.producer != null) {
			producer.flush();
		}
	}
}