package com.mapr.examples;

import com.google.common.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class Producer {
  public static void main(String[] args) throws IOException {
    // set up the producer
    KafkaProducer<String, byte[]> producer;
    try (InputStream props = Resources.getResource("producer.props").openStream()) {
      Properties properties = new Properties();
      properties.load(props);
      producer = new KafkaProducer<>(properties);
    }


    try {
      byte[] fileData = Files.readAllBytes(FileSystems.getDefault().getPath("src/test.JPG"));
      ArrayList<byte[]> allChunks = splitFile(fileData);
      for (byte[] allChunk : allChunks) {
        ProducerRecord<String, byte[]> data = new ProducerRecord<>("test", "test.JPG", allChunk);
        producer.send(data);
      }
      System.out.println("Published file test.JPG");
    } catch (Throwable throwable) {
      System.out.printf("%s", throwable.getStackTrace());
    } finally {
      producer.close();
    }

  }

  private static ArrayList<byte[]> splitFile(byte[] datum) {
    int i, l = datum.length;
    int block = 10240;
    int numblocks = l / block;
    int marker = 0;
    byte[] chunk;
    ArrayList<byte[]> data = new ArrayList<>();
    for (i = 0; i < numblocks; i++) {
      chunk = Arrays.copyOfRange(datum, marker, marker + block);
      data.add(chunk);
      marker += block;
    }
    chunk = Arrays.copyOfRange(datum, marker, l);
    data.add(chunk);
    data.add(null);
    return data;
  }

}
