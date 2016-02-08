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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenerateIntoManyKafkaTopics {

	private static final Logger LOG = LoggerFactory.getLogger(GenerateIntoManyKafkaTopics.class);

	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		ParameterTool pt = ParameterTool.fromPropertiesFile(args[0]);
		String topicPrefix = pt.getRequired("topicPrefix");
		final long[] messagesPerTopic = {pt.getLong("messagesPerTopic")};

		SerializationSchema<Message> messageSer =
				new TypeInformationSerializationSchema<>((TypeInformation<Message>) TypeExtractor.createTypeInfo(Message.class),
						env.getConfig());

		int topicCount = pt.getInt("topicCount");
		final long sleep = pt.getLong("sleep", 0);
		for(int i = 0; i < topicCount; i++) {
			final String topic = topicPrefix + i;
			DataStream<Message> stream = env.addSource(new SourceFunction<Message>() {
				public boolean running = true;

				@Override
				public void run(SourceContext<Message> ctx) throws Exception {
					while(running) {
						if(messagesPerTopic[0]-- == 0) {
							LOG.info("Reached end");
							running = false;
							break;
						}
						if(sleep > 0) {
							Thread.sleep(sleep);
						}
						Message msg = new Message();
						msg.id = messagesPerTopic[0];
						msg.topic = topic;
						ctx.collect(msg);
					}
				}

				@Override
				public void cancel() {
					running = false;
				}
			});
			stream.addSink(new FlinkKafkaProducer08<>(topic, messageSer, pt.getProperties()));
		}



		// execute program
		env.execute("Streaming data into " + topicCount + " topics");
	}
}
