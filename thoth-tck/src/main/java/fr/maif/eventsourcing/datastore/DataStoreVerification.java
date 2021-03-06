package fr.maif.eventsourcing.datastore;

import akka.actor.ActorSystem;
import akka.stream.javadsl.Sink;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.EventProcessor;
import fr.maif.eventsourcing.EventStore;
import fr.maif.eventsourcing.ProcessingSuccess;
import fr.maif.eventsourcing.format.JacksonSimpleFormat;
import fr.maif.json.EventEnvelopeJson;
import io.vavr.Tuple0;
import io.vavr.control.Either;
import io.vavr.control.Option;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public abstract class DataStoreVerification<TxCtx> implements DataStoreVerificationRules<TestState, TestEvent, Tuple0, Tuple0, TxCtx>{
    public ActorSystem actorSystem = ActorSystem.create();
    public abstract EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor(String topic);
    public abstract String kafkaBootstrapUrl();

    @Override
    @Test
    public void required_submitValidSingleEventCommandMustWriteEventInDataStore() {
        String topic = randomKafkaTopic();
        final EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor = eventProcessor(topic);
        submitValidCommand(eventProcessor, "1");

        List<EventEnvelope<TestEvent, Tuple0, Tuple0>> envelopes = deduplicateOnId(readFromDataStore(eventProcessor.eventStore()));

        cleanup(eventProcessor);
        assertThat(envelopes).hasSize(1);
    }

    @Override
    @Test
    public void required_submitInvalidCommandMustNotWriteEventsIntDataStore() {
        String topic = randomKafkaTopic();
        final EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor = eventProcessor(topic);
        submitInvalidCommand(eventProcessor, "1");

        List<EventEnvelope<TestEvent, Tuple0, Tuple0>> envelopes = deduplicateOnId(readFromDataStore(eventProcessor.eventStore()));

        cleanup(eventProcessor);
        assertThat(envelopes).isEmpty();
    }

    @Override
    @Test
    public void required_submitMultiEventCommandMustWriteAllEventsInDataStore() {
        String topic = randomKafkaTopic();
        final EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor = eventProcessor(topic);
        submitMultiEventsCommand(eventProcessor, "1");

        List<EventEnvelope<TestEvent, Tuple0, Tuple0>> envelopes = deduplicateOnId(readFromDataStore(eventProcessor.eventStore()));

        cleanup(eventProcessor);

        assertThat(envelopes.size()).isGreaterThan(1);
        List<UUID> ids = envelopes.stream().map(envelope -> envelope.id).collect(Collectors.toList());

        assertThat(ids).doesNotHaveDuplicates();
    }

    @Override
    @Test
    public void required_aggregateOfSingleEventStateShouldBeCorrect() {
        String topic = randomKafkaTopic();
        EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor = eventProcessor(topic);

        submitValidCommand(eventProcessor, "1");
        Option<TestState> state = readState(eventProcessor, "1");

        cleanup(eventProcessor);

        assertThat(state.isDefined()).isTrue();
        assertThat(state.get().count).isEqualTo(1);
    }

    @Override
    @Test
    public void required_aggregateOfDeleteEventStateShouldBeEmpty() {
        String topic = randomKafkaTopic();
        EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor = eventProcessor(topic);

        submitValidCommand(eventProcessor, "1");
        submitDeleteCommand(eventProcessor, "1");
        Option<TestState> state = readState(eventProcessor, "1");

        cleanup(eventProcessor);

        assertThat(state.isEmpty()).isTrue();
    }

    @Override
    @Test
    public void required_aggregateOfMultipleEventStateShouldBeCorrect() {
        String topic = randomKafkaTopic();
        EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor = eventProcessor(topic);

        submitMultiEventsCommand(eventProcessor, "1");
        Option<TestState> state = readState(eventProcessor, "1");

        cleanup(eventProcessor);

        assertThat(state.isDefined()).isTrue();
        assertThat(state.get().count).isEqualTo(2);
    }

    @Override
    @Test
    public void required_singleEventShouldBePublished() {
        String topic = randomKafkaTopic();
        EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor = eventProcessor(topic);

        submitValidCommand(eventProcessor, "1");
        List<EventEnvelope<TestEvent, Tuple0, Tuple0>> envelopes = deduplicateOnId(readPublishedEvents(kafkaBootstrapUrl(), topic));

        cleanup(eventProcessor);

        assertThat(envelopes).hasSize(1);
    }

    @Override
    @Test
    public void required_multipleEventsShouldBePublished() {
        String topic = randomKafkaTopic();
        EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor = eventProcessor(topic);
        submitMultiEventsCommand(eventProcessor, "1");
        List<EventEnvelope<TestEvent, Tuple0, Tuple0>> envelopes = deduplicateOnId(readPublishedEvents(kafkaBootstrapUrl(), topic));

        cleanup(eventProcessor);

        assertThat(envelopes).hasSize(2);
    }

    @Override
    @Test
    public void required_eventShouldBePublishedEventIfBrokerIsDownAtFirst() {
        String topic = randomKafkaTopic();
        EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor = eventProcessor(topic);
        shutdownBroker();
        submitValidCommand(eventProcessor, "1");

        restartBroker();
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        List<EventEnvelope<TestEvent, Tuple0, Tuple0>> envelopes = deduplicateOnId(readPublishedEvents(kafkaBootstrapUrl(), topic));

        cleanup(eventProcessor);

        assertThat(envelopes).hasSize(1);
    }

    @Override
    @Test
    public void required_commandSubmissionShouldFailIfDatabaseIsNotAvailable() {
        String topic = randomKafkaTopic();
        EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor = eventProcessor(topic);
        shutdownDatabase();
        try {
            Either<String, ProcessingSuccess<TestState, TestEvent, Tuple0, Tuple0, Tuple0>> result = submitValidCommand(eventProcessor, "1");

            cleanup(eventProcessor);

            assertThat(result.isLeft()).isTrue();
        } catch(Throwable ex) {
            // implementation should either return an embedded error in either, either throw an exception
        }finally {
            restartDatabase();
        }
    }

    @Override
    public Either<String, ProcessingSuccess<TestState, TestEvent, Tuple0, Tuple0, Tuple0>> submitValidCommand(
            EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor,
            String id) {
        return eventProcessor.processCommand(new TestCommand.SimpleCommand(id)).get();
    }

    @Override
    public void submitInvalidCommand(
            EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor,
            String id
    ) {
        eventProcessor.processCommand(new TestCommand.InvalidCommand(id)).get();

    }

    @Override
    public void submitMultiEventsCommand(
            EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor,
            String id
    ) {
        eventProcessor.processCommand(new TestCommand.MultiEventCommand(id)).get();
    }

    @Override
    public void submitDeleteCommand(EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor, String id) {
        eventProcessor.processCommand(new TestCommand.DeleteCommand(id)).get();
    }

    @Override
    public Option<TestState> readState(EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor, String id) {
        return eventProcessor.getAggregate(id).get();
    }

    @Override
    public List<EventEnvelope<TestEvent, Tuple0, Tuple0>> readFromDataStore(EventStore<TxCtx, TestEvent, Tuple0, Tuple0> eventStore) {
        try {
            return eventStore.loadAllEvents().runWith(Sink.seq(), actorSystem).toCompletableFuture().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public String randomKafkaTopic() {
        return "test-topic" + UUID.randomUUID();
    }

    private List<EventEnvelope<TestEvent, Tuple0, Tuple0>> deduplicateOnId(List<EventEnvelope<TestEvent, Tuple0, Tuple0>> envelopes) {
        return io.vavr.collection.List.ofAll(envelopes).distinctBy(env -> env.id).toJavaList();
    }
}
