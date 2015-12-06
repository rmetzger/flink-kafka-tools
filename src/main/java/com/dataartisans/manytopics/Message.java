package com.dataartisans.manytopics;

public class Message {
	public String topic;
	public long id;

	@Override
	public String toString() {
		return "Message{" +
				"topic='" + topic + '\'' +
				", id=" + id +
				'}';
	}
}
