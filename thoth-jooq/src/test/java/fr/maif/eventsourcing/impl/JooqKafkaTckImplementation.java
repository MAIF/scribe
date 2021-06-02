package fr.maif.eventsourcing.impl;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import fr.maif.eventsourcing.Projection;
import fr.maif.eventsourcing.datastore.TestConsistentProjection;
import fr.maif.eventsourcing.datastore.TestInstransactionProjection;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import akka.actor.ActorSystem;
import akka.kafka.ProducerSettings;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.EventProcessor;
import fr.maif.eventsourcing.PostgresKafkaEventProcessor;
import fr.maif.eventsourcing.datastore.DataStoreVerification;
import fr.maif.eventsourcing.datastore.TestCommand;
import fr.maif.eventsourcing.datastore.TestCommandHandler;
import fr.maif.eventsourcing.datastore.TestEvent;
import fr.maif.eventsourcing.datastore.TestEventFormat;
import fr.maif.eventsourcing.datastore.TestEventHandler;
import fr.maif.eventsourcing.datastore.TestState;
import fr.maif.eventsourcing.format.JacksonEventFormat;
import fr.maif.eventsourcing.format.JacksonSimpleFormat;
import fr.maif.json.EventEnvelopeJson;
import fr.maif.kafka.JsonSerializer;
import fr.maif.kafka.KafkaSettings;
import io.vavr.Tuple0;

public class JooqKafkaTckImplementation extends DataStoreVerification<Connection> {
    private PGSimpleDataSource dataSource;
    private TableNames tableNames;
    private TestEventFormat eventFormat;
    private PostgreSQLContainer postgres;
    private KafkaContainer kafka;
    private Projection testProjection;

    private final String SCHEMA = "CREATE TABLE IF NOT EXISTS test_journal (\n" +
            "                      id UUID primary key,\n" +
            "                      entity_id varchar(100) not null,\n" +
            "                      sequence_num bigint not null,\n" +
            "                      event_type varchar(100) not null,\n" +
            "                      version int not null,\n" +
            "                      transaction_id varchar(100) not null,\n" +
            "                      event jsonb not null,\n" +
            "                      metadata jsonb,\n" +
            "                      context jsonb,\n" +
            "                      total_message_in_transaction int default 1,\n" +
            "                      num_message_in_transaction int default 1,\n" +
            "                      emission_date timestamp not null default now(),\n" +
            "                      user_id varchar(100),\n" +
            "                      system_id varchar(100),\n" +
            "                      published boolean default false,\n" +
            "                      UNIQUE (entity_id, sequence_num)\n" +
            "                    );\n" +
            "                        \n" +
            "                    CREATE SEQUENCE if not exists test_sequence_num;";
    private final String TRUNCATE_QUERY = "TRUNCATE TABLE test_journal";

    @AfterClass(alwaysRun = true)
    public void tearDown() throws InterruptedException {
        Thread.sleep(10000);
        postgres.stop();
        kafka.stop();
    }

    @BeforeClass(alwaysRun = true)
    public void initClass() {

        this.tableNames = new TableNames("test_journal", "test_sequence_num");
        this.eventFormat = new TestEventFormat();

        postgres = new PostgreSQLContainer();
        postgres.start();
        kafka = new KafkaContainer();
        kafka.start();
        consistentProjection = new TestConsistentProjection(actorSystem,kafka.getBootstrapServers(),eventFormat,dataSource);
    }


    @BeforeMethod(alwaysRun = true)
    public void init() throws SQLException {
        this.testProjection = new TestInstransactionProjection();
        this.dataSource = new PGSimpleDataSource();
        dataSource.setUrl(postgres.getJdbcUrl());
        dataSource.setUser(postgres.getUsername());
        dataSource.setPassword(postgres.getPassword());
        // Override default setting, which wait indefinitely if database is down
        dataSource.setLoginTimeout(5);

        dataSource.getConnection().prepareStatement(SCHEMA).execute();
        dataSource.getConnection().prepareStatement(TRUNCATE_QUERY).execute();
    }

    @Override
    public EventProcessor<String, TestState, TestCommand, TestEvent, Connection, Tuple0, Tuple0, Tuple0> eventProcessor(String topic) {

        final PostgresKafkaEventProcessor<String, TestState, TestCommand, TestEvent, Tuple0, Tuple0, Tuple0> eventProcessor = PostgresKafkaEventProcessor
                .withActorSystem(ActorSystem.create())
                .withDataSource(dataSource)
                .withTables(tableNames)
                .withTransactionManager(Executors.newFixedThreadPool(4))
                .withEventFormater(eventFormat)
                .withNoMetaFormater()
                .withNoContextFormater()
                .withKafkaSettings(topic, producerSettings(settings(), new TestEventFormat()))
                .withEventHandler(new TestEventHandler())
                .withDefaultAggregateStore()
                .withCommandHandler(new TestCommandHandler<>())
                .withProjections(this.testProjection)
                .build();


        return eventProcessor;
    }

    @Override
    public String kafkaBootstrapUrl() {
        return kafka.getBootstrapServers();
    }

    @Override
    public void shutdownBroker() {
        pauseContainer(kafka);
    }

    @Override
    public void restartBroker() {
        unPauseContainer(kafka);
    }

    @Override
    public void shutdownDatabase() {
        pauseContainer(postgres);
    }

    @Override
    public void restartDatabase() {
        unPauseContainer(postgres);
    }

    private void pauseContainer(GenericContainer container) {
        container.getDockerClient().pauseContainerCmd(container.getContainerId()).exec();
    }

    private void unPauseContainer(GenericContainer container) {
        container.getDockerClient().unpauseContainerCmd(container.getContainerId()).exec();
    }

    private KafkaSettings settings() {
        return KafkaSettings.newBuilder(kafka.getBootstrapServers()).build();
    }

    private ProducerSettings<String, EventEnvelope<TestEvent, Tuple0, Tuple0>> producerSettings(
            KafkaSettings kafkaSettings,
            JacksonEventFormat<String, TestEvent> eventFormat) {
        return kafkaSettings.producerSettings(actorSystem, JsonSerializer.of(
                eventFormat,
                JacksonSimpleFormat.empty(),
                JacksonSimpleFormat.empty()
                )
        );
    }

    @Override
    public List<EventEnvelope<TestEvent, Tuple0, Tuple0>> readPublishedEvents(String kafkaBootstrapUrl, String topic) {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        String groupId = "reader-" + UUID.randomUUID();
        Optional<Long> maybeLastOffset =  getEndOffsetIfNotReached(topic, kafkaBootstrapUrl, groupId);
        if(!maybeLastOffset.isPresent()) {
            return Collections.emptyList();
        }
        long lastOffset = maybeLastOffset.get();

        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBootstrapUrl);
        props.put("group.id", groupId);
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(topic));

        boolean running = true;
        List<EventEnvelope<TestEvent, Tuple0, Tuple0>> envelopes = new ArrayList<>();
        while(running) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> record : records) {
                final long offset = record.offset();
                if (offset >= lastOffset) {
                    running = false;
                }
                envelopes.add(parsEnvelope(record.value()));
            }
            consumer.commitSync();
        }
        consumer.close();
        return envelopes;
    }

    @Override
    public Integer readProjection() {
        return ((TestInstransactionProjection)this.testProjection).getCount();
    }

    @Override
    public Integer readConsistentProjection() {
        return consistentProjection.getCount();
    }

    private static Optional<Long> getEndOffsetIfNotReached(String topic, String kafkaServers, String groupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<?, ?> consumer = new KafkaConsumer<>(properties);
        PartitionInfo partitionInfo = consumer.partitionsFor("foo").get(0);
        TopicPartition topicPartition = new TopicPartition(topic, partitionInfo.partition());
        consumer.assign(Collections.singletonList(topicPartition));

        long position = consumer.position(topicPartition);
        consumer.seekToEnd(Collections.singletonList(topicPartition));
        final long endOffset = consumer.position(topicPartition);

        Optional<Long> result = Optional.empty();
        if(endOffset > 0 && endOffset > position) {
            result = Optional.of(consumer.position(topicPartition) - 1);
        }

        consumer.close();
        return result;
    }

    public EventEnvelope<TestEvent, Tuple0, Tuple0> parsEnvelope(String value) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode node = (ObjectNode) mapper.readTree(value);
            CompletableFuture<EventEnvelope<TestEvent, Tuple0, Tuple0>> future = new CompletableFuture<>();
            EventEnvelopeJson.deserialize(
                    node,
                    eventFormat,
                    JacksonSimpleFormat.empty(),
                    JacksonSimpleFormat.empty(),
                    (event, err) -> {
                        future.completeExceptionally(new RuntimeException(err.toString()));
                    },
                    future::complete
            );
            return future.get();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
