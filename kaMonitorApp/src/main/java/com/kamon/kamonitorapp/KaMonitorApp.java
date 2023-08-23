package com.kamon.kamonitorapp;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Api
public class KaMonitorApp extends Application {

    private TextArea clusterInfoArea;
    private TextField topicField;
    private TextArea messageArea;

    public static void main(String[] args) {
        launch(args);
    }

    @Override
    public void start(Stage primaryStage) {
        primaryStage.setTitle("Kafka Monitor UI");

        clusterInfoArea = new TextArea();
        clusterInfoArea.setPrefRowCount(5);
        clusterInfoArea.setEditable(false);

        Label topicLabel = new Label("Topic:");
        topicField = new TextField();

        Label messageLabel = new Label("Message:");
        messageArea = new TextArea();
        messageArea.setPrefRowCount(5);

        Button fetchButton = new Button("Fetch Cluster Info");
        fetchButton.setOnAction(event -> fetchClusterInfo());

        Button sendButton = new Button("Send Message");
        sendButton.setOnAction(event -> sendMessage());

        VBox vbox = new VBox(clusterInfoArea, topicLabel, topicField, messageLabel, messageArea, fetchButton, sendButton);
        Scene scene = new Scene(vbox, 400, 300);

        primaryStage.setScene(scene);
        primaryStage.show();
    }

    @ApiOperation(value = "Fetch cluster information")
    private void fetchClusterInfo() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        try (AdminClient adminClient = AdminClient.create(props)) {
            DescribeClusterResult describeClusterResult = adminClient.describeCluster();

            StringBuilder infoBuilder = new StringBuilder();
            infoBuilder.append("Cluster ID: ").append(describeClusterResult.clusterId().get()).append("\n");

            Node controller = describeClusterResult.controller().get();
            infoBuilder.append("Controller Node: ").append(controller.id()).append(" - ").append(controller.host()).append(":").append(controller.port()).append("\n");

            infoBuilder.append("Broker List:\n");
            describeClusterResult.nodes().get().forEach(node -> {
                infoBuilder.append("Broker ").append(node.id()).append(": ").append(node.host()).append(":").append(node.port()).append("\n");
               // BrokerMetadata brokerMetadata = describeClusterResult.brokerMetadata(node.id()).get();
                Node brokerMetadata = null;
                try {
                    brokerMetadata = describeClusterResult.controller().get();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }
                infoBuilder.append("   Rack: ").append(brokerMetadata.rack()).append("\n");
            });

            clusterInfoArea.setText(infoBuilder.toString());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            clusterInfoArea.setText("Error retrieving cluster info");
        }
    }

    @ApiOperation(value = "Send message to Kafka topic")
    private void sendMessage() {
        String topic = topicField.getText();
        String message = messageArea.getText();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(topic, message));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

