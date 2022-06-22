package com.njit.recognition;

import com.amazon.sqs.javamessaging.ProviderConfiguration;
import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;

import javax.jms.JMSException;

@SpringBootApplication
public class RecognitionApplication {

    public static void main(String[] args) {
        SpringApplication.run(RecognitionApplication.class, args);
    }

    @Bean
    public AmazonS3 getAmazonS3() {
        return AmazonS3ClientBuilder.standard().build();
    }

    @Bean
    public SQSConnection getSQSConnection() throws JMSException {
        SQSConnectionFactory connectionFactory = new SQSConnectionFactory(new ProviderConfiguration(), AmazonSQSClientBuilder.defaultClient());
        return connectionFactory.createConnection();
    }

}
