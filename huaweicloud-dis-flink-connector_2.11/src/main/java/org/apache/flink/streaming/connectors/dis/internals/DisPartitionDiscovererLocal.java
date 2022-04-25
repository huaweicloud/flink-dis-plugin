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
 * A partition discoverer that can be used to discover streams and partitions metadata
 * from DIS using DIS Kafka Adapter, and you should get partitions by index Of Subtask.
 *
 * @see {@link DisPartitionHolder}
 */
@Internal
public class DisPartitionDiscovererLocal extends AbstractPartitionDiscoverer {

    private final Properties disProperties;

    private final DisStreamsDescriptor topicsDescriptor;

    private DisPartitionHolder disPartitionHolder;

    private static DisPartitionDiscovererLocal instance;

    public DisPartitionDiscovererLocal(
            DisStreamsDescriptor topicsDescriptor,
            int indexOfThisSubtask,
            int numParallelSubtasks,
            Properties kafkaProperties) {

        super(topicsDescriptor, indexOfThisSubtask, numParallelSubtasks);
        this.topicsDescriptor = topicsDescriptor;
        this.disProperties = checkNotNull(kafkaProperties);
        this.disPartitionHolder = DisPartitionHolder.getInstance(topicsDescriptor, disProperties);
    }

    @Override
    protected void initializeConnections() {
        // Nothing to do
    }

    @Override
    protected List<String> getAllTopics() throws WakeupException {
//        return disPartitionHolder.getAllTopics();
        // Action is not supported right now
        throw new UnsupportedOperationException();
    }

    @Override
    protected List<DisStreamPartition> getAllPartitionsForTopics(List<String> topics) throws WakeupException {
        // Action is not supported right now
        throw new UnsupportedOperationException();
    }

    @Override
    protected void wakeupConnections() {
        // Nothing to do
    }

    @Override
    protected void closeConnections() throws Exception {
        this.disPartitionHolder.close();
    }
}
