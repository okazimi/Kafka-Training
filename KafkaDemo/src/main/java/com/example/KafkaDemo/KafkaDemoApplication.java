package com.example.KafkaDemo;


import com.example.KafkaDemo.consumer.ConsumerGroup;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class KafkaDemoApplication {

	public static void main(String[] args) throws Exception {
		// Generate 3 Topics with 3 partitions each
		// args[0] = number of topics per group
		int numTopics = Integer.parseInt(args[0]);
		// args[1] = number of groups
		int numGroups = Integer.parseInt(args[1]);

		ArrayList<String> topicList = new ArrayList<>();
		for(int i = 1; i < numTopics+1; i++){
			topicList.add("topic-" + Integer.toString(i));
			TopicCreator.run("topic-" + Integer.toString(i), 3);
		}

		ArrayList<String> groupList = new ArrayList<>();
		for(int i = 1; i < numGroups+1; i++){
			groupList.add("consumer-group-" + Integer.toString(i));
			TopicCreator.run("topic-" + Integer.toString(i), 3);
		}
		runGroups(numGroups, groupList, topicList);
//		ConsumerGroup.run(1, groupList);

	}
	public static void runGroups(int consumerCount, ArrayList<String> groups, ArrayList<String> topics) throws Exception{
		ConsumerGroup.run(consumerCount, groups, topics);
	}

}
