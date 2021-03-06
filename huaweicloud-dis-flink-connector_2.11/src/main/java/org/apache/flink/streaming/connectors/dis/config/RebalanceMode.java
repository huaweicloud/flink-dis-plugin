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

package org.apache.flink.streaming.connectors.dis.config;

import org.apache.flink.annotation.Internal;

/**
 * 给任务分配Partition的方式。
 */
@Internal
public enum RebalanceMode {

	/** 单线程加入Consumer Group，在客户端完成Partition分配。 */
	CLIENT,

	/** 多线程加入Consumer Group，在服务端完成Rebalance，目前Rebalance时间过长，不建议使用 */
	SERVER;
}
