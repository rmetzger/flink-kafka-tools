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

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class ReadFromManyKafkaTopics {

	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		ParameterTool pt = ParameterTool.fromPropertiesFile(args[0]);
		String topicPrefix = pt.getRequired("topicPrefix");

		DeserializationSchema<Message> messageSer =
				new TypeInformationSerializationSchema<>((TypeInformation<Message>) TypeExtractor.createTypeInfo(Message.class),
						env.getConfig());

		final int topicCount = pt.getInt("topicCount");

		List<String> topics = new ArrayList<>();
		for(int i = 0; i < topicCount; i++) {
			final String topic = topicPrefix + i;
			topics.add(topic);
		}
		DataStream<Message> stream = env.addSource(new FlinkKafkaConsumer082<>(topics, messageSer, pt.getProperties()));

		stream.keyBy("topic").flatMap(new RichFlatMapFunction<Message, Object>() {

			@Override
			public void flatMap(Message message, Collector<Object> collector) throws Exception {
				LongCounter topicElements = getRuntimeContext().getLongCounter(message.topic + "_element_count");
				topicElements.add(1L);
			}
		});

		stream.print();


		// execute program
		env.execute("Streaming data into " + topicCount + " topics");
	}
}
