package com.dataflair.kafka.consumer;


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetFetchRequest;
import kafka.javaapi.OffsetFetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

public class KafkaLowLevelConsumer {

    public static void main(String args[]) {
    	// Create the instance of KafkaLowLevelConsumer Class. This is the sample class that we have created.
        KafkaLowLevelConsumer kafkaLowLevelConsumerexample = new KafkaLowLevelConsumer();
        // Set the number of records we want to read
        long numberOfRecords = 200;
        // Set the name of topic whose data we want to read
        String topicName = "topic4";
        // Set the partition whose data we want to read
        int partitionNumber = 1;

        // Set the groupName
        String groupName = "groupName8";
        // Set the list of broker host
        List<String> brokers = new ArrayList<String>();
        brokers.add("localhost");
        // Set the port of broker
        int port = Integer.parseInt("9092");

        try {
        	// Start the consumer
            kafkaLowLevelConsumerexample.startConsumer(numberOfRecords, topicName, partitionNumber, brokers, port,groupName);
		} catch (Exception e) {
			System.out.println("Exception:" + e);
			e.printStackTrace();
		}
    }

    private List<String> partitionReplicaBrokers = new ArrayList<String>();


    public void startConsumer(long numberOfRecords, String topicName, int partitionNumber, List<String> brokers, int port, String groupName) throws Exception {
        // Get the metadata of topic whose data we want to consume
        PartitionMetadata metadata = searchLeader(brokers, port, topicName, partitionNumber);

        // check for partition metadata details available
        if (metadata == null) {
            System.out.println("Can't find metadata for Topic and Partition. Exiting");
            return;
        }
        // check the metadata available for leader partition
        if (metadata.leader() == null) {
            System.out.println("Can't find Leader for Topic and Partition. Exiting");
            return;
        }
        // get the leader broker of the topic
        String leadBroker = metadata.leader().host();

        // Name of the client
        String clientName = "Client_" + topicName + "_" + partitionNumber;

        // create the instance of consumer to fetch data from given partition
        SimpleConsumer consumer = new SimpleConsumer(leadBroker, port, 100000, 64 * 1024, clientName);

        // read the current offset need to process for given group
        long readOffset = getGroupOffset(consumer, topicName, partitionNumber, clientName, groupName,kafka.api.OffsetRequest.EarliestTime());

        int numErrors = 0;

        // number of records read must be greater than zero
        while (numberOfRecords > 0) {
        	// if the instance of consumer is null, then create again
            if (consumer == null) {
                consumer = new SimpleConsumer(leadBroker, port, 100000, 64 * 1024, clientName);
            }

            // Note: this fetchSize of 100000 might need to be increased if large batches are written to Kafka
            int fetchSize = 100000;
            // create the fetch request
            FetchRequest req = new FetchRequestBuilder().clientId(clientName).addFetch(topicName, partitionNumber, readOffset, fetchSize).build();
            // now set the request in consumer
            FetchResponse fetchResponse = consumer.fetch(req);

            // check the fetch respnise has some error or not
            if (fetchResponse.hasError()) {
                numErrors++;
                // Some error has occured
                short code = fetchResponse.errorCode(topicName, partitionNumber);
                System.out.println("Error occure from leading the data from broker:" + leadBroker + " Reason: " + code);
                // break, if number of failed more than 5 times
                if (numErrors > 5) break;
                // if error code is OffsetOutOfRangeCode, then fetch the latest error code
                if (code == ErrorMapping.OffsetOutOfRangeCode())  {
                    readOffset = getGroupOffset(consumer, topicName, partitionNumber, clientName, groupName,kafka.api.OffsetRequest.LatestTime());
                    continue;
                }
                consumer.close();
                consumer = null;
                // check for new consumer leader
                leadBroker = searchNewLeader(leadBroker, topicName, partitionNumber, port);
                continue;
            }
            numErrors = 0;
            TopicAndPartition topicAndPartition = new TopicAndPartition(topicName, partitionNumber);
            long numRead = 0;
            // Read the data from consumer
            for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topicName, partitionNumber)) {
                long currentOffset = messageAndOffset.offset();
                if (currentOffset < readOffset) {
                    System.out.println("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
                    continue;
                }
                readOffset = messageAndOffset.nextOffset();
                ByteBuffer payload = messageAndOffset.message().payload();

                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                System.out.println("offset data read : "+String.valueOf(messageAndOffset.offset()) + " : " + new String(bytes, "UTF-8"));
                numRead++;
                if(numRead%100 == 0) {
                	commitOffset(consumer, clientName, topicAndPartition, groupName, messageAndOffset.offset() + 1);
                }
            }

            if (numRead == 0) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
        if (consumer != null) consumer.close();
    }



    public static long getLastOffset(SimpleConsumer consumer, String topic, int partition,
                                     long whichTime, String clientName) {

        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));

        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);

        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            System.out.println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition) );
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        System.out.println("*****************  "+offsets[0]);
        return offsets[0];
    }




	private static long getGroupOffset(SimpleConsumer consumer,
			String topic, int partition, String clientName, String groupName, long whichOffset) {

        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        List<TopicAndPartition> topicAndPartitions = new ArrayList<TopicAndPartition>();
        topicAndPartitions.add(topicAndPartition);

        OffsetFetchResponse offsetFetchResponse= consumer.fetchOffsets(new OffsetFetchRequest(groupName, topicAndPartitions, kafka.api.OffsetRequest.CurrentVersion(), 1, clientName));
        Long readOffset =  (Long)offsetFetchResponse.offsets().get(topicAndPartition).asTuple()._1();
        if(readOffset==-1) {
        	readOffset = getLastOffset(consumer,topic, partition, whichOffset, clientName);
        	commitOffset(consumer, clientName, topicAndPartition,groupName, readOffset);
        }

		return readOffset;
	}



	private static void commitOffset(SimpleConsumer consumer,
			String clientName, TopicAndPartition topicAndPartition, String groupName, long offset) {
		// Start New Code
        Map<TopicAndPartition,OffsetMetadataAndError> requestMap = new HashMap<TopicAndPartition, OffsetMetadataAndError>();
        requestMap.put(topicAndPartition, new OffsetMetadataAndError(offset, "metadata", (short)1));

        consumer.commitOffsets(new kafka.javaapi.OffsetCommitRequest(groupName, requestMap, kafka.api.OffsetRequest.CurrentVersion(), 1, clientName));

	}

    private String searchNewLeader(String oldLeader, String topicName, int partitionNumber, int port) throws Exception {
        for (int i = 0; i < 5; i++) {
            boolean goToSleep = false;
            // search for the broker from given broker list
            PartitionMetadata metadata = searchLeader(partitionReplicaBrokers, port, topicName, partitionNumber);
            if (metadata == null) {
                goToSleep = true;
            } else if (metadata.leader() == null) {
                goToSleep = true;
            } else if (oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                // leader details may not update on zookeeper
                goToSleep = true;
            } else {
                return metadata.leader().host();
            }
            if (goToSleep) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
        System.out.println("Not able to find broker after 5 attempts : ");
        throw new Exception("Not able to find broker after 5 attempts : ");
    }

    private PartitionMetadata searchLeader(List<String> brokers, int portNumber, String topicName, int partitionNumber) {
        PartitionMetadata returnMetaData = null;
        forLoop:
        for (String broker : brokers) {
            SimpleConsumer consumer = null;
            try {
            	// instance of consumer
                consumer = new SimpleConsumer(broker, portNumber, 100000, 64 * 1024, "leaderLookup");
                List<String> topics = Collections.singletonList(topicName);
                // Create the topic fetch request
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                // send the request to fetch the metadata
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
                List<TopicMetadata> metaData = resp.topicsMetadata();


                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                    	if (part.partitionId() == partitionNumber) {
                    		// set the parition metadata
                            returnMetaData = part;
                            break forLoop;
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("Error communicating with Broker [" + broker + "] to find Leader for [" + topicName
                        + ", " + partitionNumber + "] Reason: " + e);
            } finally {
                if (consumer != null) consumer.close();
            }
        }
        if (returnMetaData != null) {
        	// Set the partition brokers(leader + replica)
            partitionReplicaBrokers.clear();
            for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
                partitionReplicaBrokers.add(replica.host());
            }
        }
        return returnMetaData;
    }
}
