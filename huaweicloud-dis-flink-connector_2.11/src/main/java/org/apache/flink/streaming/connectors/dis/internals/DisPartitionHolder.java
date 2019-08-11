package org.apache.flink.streaming.connectors.dis.internals;

import com.huaweicloud.dis.adapter.kafka.clients.consumer.Consumer;
import com.huaweicloud.dis.adapter.kafka.clients.consumer.ConsumerRebalanceListener;
import com.huaweicloud.dis.adapter.kafka.clients.consumer.DISKafkaConsumer;
import com.huaweicloud.dis.adapter.kafka.common.TopicPartition;
import org.apache.flink.streaming.connectors.dis.FlinkDisConsumer;

import java.util.*;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.PropertiesUtil.getLong;

/**
 * 保存Partition信息和 {@link DISKafkaConsumer} 实例，{@link DisPartitionDiscovererLocal} 将从这里获取
 * 更新后的Partition信息并分配给子任务。
 */
public class DisPartitionHolder {

    private final Properties disProperties;

    private final DisStreamsDescriptor topicsDescriptor;

    private List<DisStreamPartition> allPartitions = new LinkedList<>();

    List<String> allTopics;

    private Thread disPartitionDiscoverThread;

    private Consumer<?, ?> disKafkaConsumer;

    private static DisPartitionHolder instance;

    private DisPartitionHolder(DisStreamsDescriptor topicsDescriptor,
            Properties disProperties) {
        this.disProperties = checkNotNull(disProperties);
        this.topicsDescriptor = topicsDescriptor;

        this.disKafkaConsumer = new DISKafkaConsumer<>(disProperties);

        ConsumerRebalanceListener consumerRebalanceListener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                disKafkaConsumer.pause(collection);
            }
        };
        if (this.topicsDescriptor.isFixedTopics()) {
            this.disKafkaConsumer.subscribe(topicsDescriptor.getFixedTopics(), consumerRebalanceListener);
        } else if (this.topicsDescriptor.isTopicPattern()) {
            this.disKafkaConsumer.subscribe(this.topicsDescriptor.getTopicPattern(), consumerRebalanceListener);
        } else {
            throw new IllegalArgumentException("Illegal " + topicsDescriptor.toString());
        }

        this.disKafkaConsumer.poll(0);
        for (TopicPartition topicPartition : disKafkaConsumer.assignment()) {
            allPartitions.add(new DisStreamPartition(topicPartition.topic(), topicPartition.partition()));
        }
//        this.allTopics = new ArrayList<>(disKafkaConsumer.listTopics().keySet());

        long discoveryIntervalMillis = getLong(
                disProperties,
                FlinkDisConsumer.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, FlinkDisConsumer.PARTITION_DISCOVERY_DISABLED);
        if (discoveryIntervalMillis != FlinkDisConsumer.PARTITION_DISCOVERY_DISABLED) {
            this.disPartitionDiscoverThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        List<DisStreamPartition> newDiscoveredPartitions = new LinkedList<>();
                        disKafkaConsumer.poll(0);
                        for (TopicPartition topicPartition : disKafkaConsumer.assignment()) {
                            newDiscoveredPartitions.add(new DisStreamPartition(topicPartition.topic(), topicPartition.partition()));
                        }
                        allPartitions = newDiscoveredPartitions;
                        allTopics = new ArrayList<>(disKafkaConsumer.listTopics().keySet());

                        try {
                            Thread.sleep(discoveryIntervalMillis);
                        } catch (InterruptedException iex) {
                            // may be interrupted if the consumer was canceled midway; simply escape the loop
                            disKafkaConsumer.close();
                            break;
                        }
                    }
                }
            });
            this.disPartitionDiscoverThread.start();
        } else {
            // Partition Discovery is disabled, just close the consumer.
            this.disKafkaConsumer.close();
        }
    }

    public static synchronized DisPartitionHolder getInstance(DisStreamsDescriptor topicsDescriptor,
                                                              Properties disProperties)
    {
        if (instance == null)
        {
            instance = new DisPartitionHolder(topicsDescriptor, disProperties);
        }
        return instance;
    }

    public List<DisStreamPartition> getAllPartitions() {
        return this.allPartitions;
    }

    public List<String> getAllTopics() {
        return allTopics;
    }

    public void close() {
        synchronized (this) {
            if (this.disPartitionDiscoverThread != null && !disPartitionDiscoverThread.isInterrupted()) {
                // Shutdown the DIS Partition Discover Thread
                disPartitionDiscoverThread.interrupt();
            }
        }
    }
}
