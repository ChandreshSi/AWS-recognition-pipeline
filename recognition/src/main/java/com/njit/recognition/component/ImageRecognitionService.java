package com.njit.recognition.component;

import com.amazon.sqs.javamessaging.AmazonSQSMessagingClientWrapper;
import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import javax.jms.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Car Recognition Service (EC2)
 * This class takes the objects present in configured s3 bucket
 * Checks if the object is car
 * Put the request on the Queue to be processed by Text Recognition Service (EC2)
 */
@Component
public class ImageRecognitionService {

    private Logger logger = LoggerFactory.getLogger(ImageRecognitionService.class);

    @Autowired
    private AmazonS3 amazonS3;

    @Autowired
    private SQSConnection sqsConnection;

    @Value("${aws.bucket.name}")
    private String bucketName;

    @Value("${aws.sqs.name}")
    private String sqsName;

    @Value("${recognition.label.name}")
    private String recognitionLabelName;

    @Value("${recognition.label.confidence}")
    private int recognitionLabelConfidence;

    @EventListener(ApplicationReadyEvent.class)
    public void run() {
        try {
            logger.trace("Started processing");
            // Create SQS when not exist
            createSQS();
            Session session = getSQSSession();
            // Producer for SQS
            MessageProducer producer = createMessageProducer(session);
            // Recognition
            AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder.defaultClient();
            // S3 Bucket
            ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(this.bucketName);
            ListObjectsV2Result result = amazonS3.listObjectsV2(req);
            do {
                // for all objects present in S3
                for (S3ObjectSummary summary : result.getObjectSummaries()) {
                    List<Label> labels = detectLabel(summary.getKey(), rekognitionClient);
                    for (Label label : labels) {
                        // send message if label is a car
                        if (!recognitionLabelName.equals(label.getName()) || label.getConfidence() < recognitionLabelConfidence)
                            continue;
                        logger.info("Detected label " + recognitionLabelName + " for image: " + summary.getKey());
                        logger.info("Detected label: " + label.getName() + " confidence" + label.getConfidence());
                        sendMessage(summary.getKey(), session, producer);
                    }
                }
                result = amazonS3.listObjectsV2(req);
            } while (result.isTruncated());
            // end of queue
            sendMessage("-1", session, producer);
            logger.trace("End processing");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void createSQS() throws JMSException {
        AmazonSQSMessagingClientWrapper sqsClient = sqsConnection.getWrappedAmazonSQSClient();
        if (!sqsClient.queueExists(sqsName)) {
            Map<String, String> attributes = new HashMap<String, String>();
            attributes.put("FifoQueue", "true");
            attributes.put("ContentBasedDeduplication", "true");
            sqsClient.createQueue(new CreateQueueRequest().withQueueName(sqsName));
        }
    }

    private List<Label> detectLabel(String imageName, AmazonRekognition rekognitionClient) {
        DetectLabelsRequest request = new DetectLabelsRequest()
                .withImage(new Image().withS3Object(new S3Object().withName(imageName).withBucket(bucketName)))
                .withMaxLabels(10).withMinConfidence(75F);
        DetectLabelsResult labelsResult = rekognitionClient.detectLabels(request);
        return labelsResult.getLabels();
    }

    private void sendMessage(String imageName, Session session, MessageProducer producer) throws JMSException {
        logger.info("Sending message to SQS for: " + imageName);
        TextMessage message = session.createTextMessage(imageName + UUID.randomUUID().toString());
        message.setStringProperty("JMSXGroupID", UUID.randomUUID().toString());
        producer.send(message);
        logger.info("JMS Message " + message.getJMSMessageID());
        logger.info("JMS Message Sequence Number " + message.getStringProperty("JMS_SQS_SequenceNumber"));
    }

    private Session getSQSSession() throws JMSException {
        return sqsConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    private MessageProducer createMessageProducer(Session session) throws JMSException {
        Queue queue = session.createQueue(sqsName);
        return session.createProducer(queue);
    }

}
