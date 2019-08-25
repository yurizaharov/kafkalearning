package ru.app.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Scanner;

public class kafkaProducer {

    public static void main(String[] args) {
        Scanner scan = new Scanner(System.in);
        Properties properties = new Properties();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("acks", "1");
        properties.put("retries", "3");
        properties.put("compression.type", "snappy");

        Producer<String, String> producer = new KafkaProducer<>(properties);
        while (scan.hasNext()) {
            Date date = new Date();
            String strDate = dateFormat.format(date);
            String message = scan.nextLine();
            producer.send(new ProducerRecord<String, String>("first-topic", strDate +" "+ message));

        }
        producer.close();
    }
}