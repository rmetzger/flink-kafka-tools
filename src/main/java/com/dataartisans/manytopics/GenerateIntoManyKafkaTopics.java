package com.dataartisans.manytopics;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;

public class GenerateIntoManyKafkaTopics {

	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		ParameterTool pt = ParameterTool.fromPropertiesFile(args[0]);
		String topicPrefix = pt.getRequired("topicPrefix");
		final int messagesPerTopic = pt.getInt("messagesPerTopic");

		for(int i = 0; i < pt.getInt("topicCount"); i++) {
			env.addSource(new SourceFunction<Message>() {
				@Override
				public void run(SourceContext<Message> ctx) throws Exception {
					while(running) {
						if(messagesPerTopic++)
					}
				}

				@Override
				public void cancel() {

				}
			})
		}



		// execute program
		env.execute("Flink Java API Skeleton");
	}
}
