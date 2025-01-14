/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.processor.internals.assignment;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.streams.processor.internals.InternalTopicManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RackUtils {

    private static final Logger LOG = LoggerFactory.getLogger(RackUtils.class);

    private RackUtils() { }

    public static Map<TopicPartition, Set<String>> getRacksForTopicPartition(final Cluster cluster,
                                                                             final InternalTopicManager internalTopicManager,
                                                                             final Set<TopicPartition> topicPartitions,
                                                                             final boolean isChangelog) {
        final Set<String> topicsToDescribe = new HashSet<>();
        if (isChangelog) {
            topicsToDescribe.addAll(topicPartitions.stream().map(TopicPartition::topic).collect(
                Collectors.toSet()));
        } else {
            topicsToDescribe.addAll(topicsWithMissingMetadata(cluster, topicPartitions));
        }

        final Set<TopicPartition> topicsWithUpToDateMetadata = topicPartitions.stream()
            .filter(partition -> !topicsToDescribe.contains(partition.topic()))
            .collect(Collectors.toSet());
        final Map<TopicPartition, Set<String>> racksForTopicPartition = knownRacksForPartition(
            cluster, topicsWithUpToDateMetadata);

        final Map<String, List<TopicPartitionInfo>> freshTopicPartitionInfo =
            describeTopics(internalTopicManager, topicsToDescribe);
        freshTopicPartitionInfo.forEach((topic, partitionInfos) -> {
            for (final TopicPartitionInfo partitionInfo : partitionInfos) {
                final int partition = partitionInfo.partition();
                final TopicPartition topicPartition = new TopicPartition(topic, partition);
                final List<Node> replicas = partitionInfo.replicas();
                if (replicas == null || replicas.isEmpty()) {
                    LOG.error("No replicas found for topic partition {}: {}", topic, partition);
                    continue;
                }

                final Set<String> racks = replicas.stream().filter(Node::hasRack).map(Node::rack).collect(
                    Collectors.toSet());
                racksForTopicPartition.computeIfAbsent(topicPartition, k -> new HashSet<>());
                racksForTopicPartition.get(topicPartition).addAll(racks);
            }
        });

        return racksForTopicPartition;
    }

    public static Set<String> topicsWithMissingMetadata(final Cluster cluster, final Set<TopicPartition> topicPartitions) {
        final Set<String> topicsWithStaleMetadata = new HashSet<>();
        for (final TopicPartition topicPartition : topicPartitions) {
            final PartitionInfo partitionInfo = cluster.partition(topicPartition);
            if (partitionInfo == null) {
                LOG.error("TopicPartition {} doesn't exist in cluster", topicPartition);
                continue;
            }
            final Node[] replica = partitionInfo.replicas();
            if (replica == null || replica.length == 0) {
                topicsWithStaleMetadata.add(topicPartition.topic());
            }
        }
        return topicsWithStaleMetadata;
    }

    public static Map<TopicPartition, Set<String>> knownRacksForPartition(final Cluster cluster, final Set<TopicPartition> topicPartitions) {
        final Map<TopicPartition, Set<String>> racksForPartition = new HashMap<>();
        for (final TopicPartition topicPartition : topicPartitions) {
            final PartitionInfo partitionInfo = cluster.partition(topicPartition);
            if (partitionInfo == null) {
                LOG.error("TopicPartition {} doesn't exist in cluster", topicPartition);
                continue;
            }
            final Node[] replicas = partitionInfo.replicas();
            if (replicas == null || replicas.length == 0) {
                continue;
            }

            Arrays.stream(replicas).filter(node -> !node.hasRack()).forEach(node -> {
                LOG.warn("Node {} for topic partition {} doesn't have rack", node, topicPartition);
            });
            final Set<String> racks = Arrays.stream(replicas).filter(Node::hasRack)
                .map(Node::rack).collect(Collectors.toSet());
            racksForPartition.put(topicPartition, racks);
        }
        return racksForPartition;
    }

    private static Map<String, List<TopicPartitionInfo>> describeTopics(final InternalTopicManager internalTopicManager,
                                                                        final Set<String> topicsToDescribe) {
        if (topicsToDescribe.isEmpty()) {
            return new HashMap<>();
        }

        try {
            final Map<String, List<TopicPartitionInfo>> topicPartitionInfo = internalTopicManager.getTopicPartitionInfo(topicsToDescribe);
            if (topicsToDescribe.size() > topicPartitionInfo.size()) {
                topicsToDescribe.removeAll(topicPartitionInfo.keySet());
                LOG.error("Failed to describe topic for {}", topicsToDescribe);
            }
            return topicPartitionInfo;
        } catch (final Exception e) {
            LOG.error("Failed to describe topics {}", topicsToDescribe, e);
            return new HashMap<>();
        }
    }
}
