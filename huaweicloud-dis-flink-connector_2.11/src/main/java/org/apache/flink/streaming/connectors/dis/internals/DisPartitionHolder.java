package org.apache.flink.streaming.connectors.dis.internals;

import com.huaweicloud.dis.adapter.kafka.clients.consumer.Consumer;
import com.huaweicloud.dis.adapter.kafka.clients.consumer.DISKafkaConsumer;
import com.huaweicloud.dis.adapter.kafka.common.PartitionInfo;
import org.apache.flink.streaming.connectors.dis.FlinkDisConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.PropertiesUtil.getLong;

/**
 * 保存Partition信息和 {@link DISKafkaConsumer} 实例，{@link DisPartitionDiscovererLocal} 将从这里获取
 * 更新后的Partition信息并分配给子任务。
 */
public class DisPartitionHolder {

    private static final Logger LOG = LoggerFactory.getLogger(DisPartitionHolder.class);

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
        Map<String, List<PartitionInfo>> topicPartitions = disKafkaConsumer.listTopics();
        if (this.topicsDescriptor.isFixedTopics()) {
            List<String> topicList = topicsDescriptor.getFixedTopics();
            for (Map.Entry<String, List<PartitionInfo>> entry : topicPartitions.entrySet()) {
                String streamName = entry.getKey();
                if (topicList.contains(streamName)) {
                    List<PartitionInfo> partitionInfos = entry.getValue();
                    for (PartitionInfo partitionInfo : partitionInfos) {
                        allPartitions.add(new DisStreamPartition(entry.getKey(), partitionInfo.partition()));
                    }
                }
            }
        } else if (this.topicsDescriptor.isTopicPattern()) {
            Pattern topicPattern = this.topicsDescriptor.getTopicPattern();
            for (Map.Entry<String, List<PartitionInfo>> entry : topicPartitions.entrySet()) {
                String streamName = entry.getKey();
                if (topicPattern.matcher(streamName).matches()) {
                    List<PartitionInfo> partitionInfos = entry.getValue();
                    for (PartitionInfo partitionInfo : partitionInfos) {
                        allPartitions.add(new DisStreamPartition(entry.getKey(), partitionInfo.partition()));
                    }
                }
            }
        } else {
            throw new IllegalArgumentException("Illegal " + topicsDescriptor.toString());
        }

        long discoveryIntervalMillis = getLong(
                disProperties,
                FlinkDisConsumer.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, FlinkDisConsumer.PARTITION_DISCOVERY_DISABLED);
        if (discoveryIntervalMillis != FlinkDisConsumer.PARTITION_DISCOVERY_DISABLED) {
            this.disPartitionDiscoverThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        List<DisStreamPartition> newDiscoveredPartitions = new LinkedList<>();
                        Map<String, List<PartitionInfo>> newTopicPartitions;
                        try {
                            newTopicPartitions = disKafkaConsumer.listTopics();
                        } catch (Exception e) {
                            LOG.warn("Failed to get Stream List from DIS, continue.", e);
                            continue;
                        }
                        if (topicsDescriptor.isFixedTopics()) {
                            List<String> topicList = topicsDescriptor.getFixedTopics();
                            for (Map.Entry<String, List<PartitionInfo>> entry : newTopicPartitions.entrySet()) {
                                String streamName = entry.getKey();
                                if (topicList.contains(streamName)) {
                                    List<PartitionInfo> partitionInfos = entry.getValue();
                                    for (PartitionInfo partitionInfo : partitionInfos) {
                                        newDiscoveredPartitions.add(new DisStreamPartition(entry.getKey(), partitionInfo.partition()));
                                    }
                                }
                            }
                        } else if (topicsDescriptor.isTopicPattern()) {
                            Pattern topicPattern = topicsDescriptor.getTopicPattern();
                            for (Map.Entry<String, List<PartitionInfo>> entry : newTopicPartitions.entrySet()) {
                                String streamName = entry.getKey();
                                if (topicPattern.matcher(streamName).matches()) {
                                    List<PartitionInfo> partitionInfos = entry.getValue();
                                    for (PartitionInfo partitionInfo : partitionInfos) {
                                        newDiscoveredPartitions.add(new DisStreamPartition(entry.getKey(), partitionInfo.partition()));
                                    }
                                }
                            }
                        }
                        allPartitions = newDiscoveredPartitions;
//                        allTopics = new ArrayList<>(disKafkaConsumer.listTopics().keySet());

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
                                                              Properties disProperties) {
        if (instance == null) {
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
