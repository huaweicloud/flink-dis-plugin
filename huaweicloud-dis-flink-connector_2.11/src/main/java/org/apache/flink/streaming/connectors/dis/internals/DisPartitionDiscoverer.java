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

package org.apache.flink.streaming.connectors.dis.internals;

import com.huaweicloud.dis.adapter.kafka.clients.consumer.Consumer;
import com.huaweicloud.dis.adapter.kafka.clients.consumer.ConsumerRebalanceListener;
import com.huaweicloud.dis.adapter.kafka.clients.consumer.DISKafkaConsumer;
import com.huaweicloud.dis.adapter.kafka.common.PartitionInfo;
import com.huaweicloud.dis.adapter.kafka.common.TopicPartition;
import org.apache.flink.annotation.Internal;

import java.util.*;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A partition discoverer that can be used to discover topics and partitions metadata
 * from Kafka brokers via the Kafka 0.9 high-level consumer API.
 */
@Internal
public class DisPartitionDiscoverer extends AbstractPartitionDiscoverer {

    private final Properties kafkaProperties;

    private final DisStreamsDescriptor topicsDescriptor;

    private Consumer<?, ?> kafkaConsumer;

    public DisPartitionDiscoverer(
            DisStreamsDescriptor topicsDescriptor,
            int indexOfThisSubtask,
            int numParallelSubtasks,
            Properties kafkaProperties) {

        super(topicsDescriptor, indexOfThisSubtask, numParallelSubtasks);
        this.topicsDescriptor = topicsDescriptor;
        this.kafkaProperties = checkNotNull(kafkaProperties);
    }

    @Override
    protected void initializeConnections() {
        this.kafkaConsumer = new DISKafkaConsumer<>(kafkaProperties);

        ConsumerRebalanceListener consumerRebalanceListener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                kafkaConsumer.pause(collection);
            }
        };
        if (this.topicsDescriptor.isFixedTopics()) {
            this.kafkaConsumer.subscribe(topicsDescriptor.getFixedTopics(), consumerRebalanceListener);
        } else if (this.topicsDescriptor.isTopicPattern()) {
            this.kafkaConsumer.subscribe(this.topicsDescriptor.getTopicPattern(), consumerRebalanceListener);
        } else {
            throw new IllegalArgumentException("Illegal " + topicsDescriptor.toString());
        }
    }

    @Override
    protected List<String> getAllTopics() throws WakeupException {
        return new ArrayList<>(kafkaConsumer.listTopics().keySet());
    }

    @Override
    protected List<DisStreamPartition> getAllPartitionsForTopics(List<String> topics) throws WakeupException {
        List<DisStreamPartition> partitions = new LinkedList<>();

        for (String topic : topics) {
            for (PartitionInfo partitionInfo : kafkaConsumer.partitionsFor(topic)) {
                partitions.add(new DisStreamPartition(partitionInfo.topic(), partitionInfo.partition()));
            }
        }
        return partitions;
    }

    @Override
    protected List<DisStreamPartition> getAllPartitions() throws WakeupException {
        List<DisStreamPartition> partitions = new LinkedList<>();

        this.kafkaConsumer.poll(0);
        for (TopicPartition topicPartition : kafkaConsumer.assignment()) {
            partitions.add(new DisStreamPartition(topicPartition.topic(), topicPartition.partition()));
        }

        return partitions;
    }

    @Override
    protected void wakeupConnections() {
        if (this.kafkaConsumer != null) {
            this.kafkaConsumer.wakeup();
        }
    }

    @Override
    protected void closeConnections() throws Exception {
        if (this.kafkaConsumer != null) {
            this.kafkaConsumer.close();

            // de-reference the consumer to avoid closing multiple times
            this.kafkaConsumer = null;
        }
    }
}
