package com.mapr.examples;

import com.google.common.io.Resources;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.*;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;


public class Consumer {
  public static void main(String[] args) throws IOException {

    KafkaConsumer<String, byte[]> consumer;
    try (InputStream props = Resources.getResource("consumer.props").openStream()) {
      Properties properties = new Properties();
      properties.load(props);
      if (properties.getProperty("group.id") == null) {
        properties.setProperty("group.id", "group-" + new Random().nextInt(100000));
      }
      consumer = new KafkaConsumer<>(properties);
    }
    consumer.subscribe(Collections.singletonList("test"));

    String name=null;
    ByteArrayOutputStream bos=new ByteArrayOutputStream();
    while (true) {
      ConsumerRecords<String, byte[]> records = consumer.poll(100);
      for (ConsumerRecord<String, byte[]> record : records) {
        if (record.value()!=null) {
          name=record.key();
          bos.write(record.value());
        }
        else {
          System.out.println("File read complete "+name);
          writeFile(name+"-result.JPG",bos.toByteArray());
          bos.reset();
        }
      }
    }

  }

  private static void writeFile(String name,byte[] rawdata) throws IOException
  {

    File f=new File("src/output");
    if (!f.exists())
      f.mkdirs();

    FileOutputStream fos=new FileOutputStream("src/output"+File.separator+name);
    fos.write(rawdata);
    fos.flush();
    fos.close();


  }
}
