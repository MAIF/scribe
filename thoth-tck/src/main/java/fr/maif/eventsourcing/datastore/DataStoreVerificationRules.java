package fr.maif.eventsourcing.datastore;

import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.EventProcessor;
import fr.maif.eventsourcing.EventStore;
import fr.maif.eventsourcing.ProcessingSuccess;
import fr.maif.eventsourcing.State;
import io.vavr.Tuple0;
import io.vavr.control.Either;
import io.vavr.control.Option;

import java.util.List;

public interface DataStoreVerificationRules<Ste extends State, Evt extends Event, Meta, Context, TxCtx> {
    Either<String, ProcessingSuccess<TestState, TestEvent, Tuple0, Tuple0, Tuple0>> submitValidCommand(EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor, String id);
    void submitInvalidCommand(EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor,  String id);
    void submitMultiEventsCommand(EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor, String id);
    Option<Ste> readState(EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor, String id);
    void submitDeleteCommand(EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor, String id);
    List<EventEnvelope<TestEvent, Tuple0, Tuple0>> readPublishedEvents(String kafkaBootstrapUrl, String topic);
    Integer readProjection();
    Integer readConsistentProjection();
    void shutdownBroker();
    void restartBroker();
    void shutdownDatabase();
    void restartDatabase();

    void required_submitValidSingleEventCommandMustWriteEventInDataStore();
    void required_submitInvalidCommandMustNotWriteEventsIntDataStore();
    void required_submitMultiEventCommandMustWriteAllEventsInDataStore();

    void required_aggregateOfSingleEventStateShouldBeCorrect();
    void required_aggregateOfMultipleEventStateShouldBeCorrect();
    void required_aggregateOfDeleteEventStateShouldBeEmpty();

    void required_singleEventShouldBePublished();
    void required_multipleEventsShouldBePublished();

    void required_eventShouldBePublishedEventIfBrokerIsDownAtFirst();
    void required_commandSubmissionShouldFailIfDatabaseIsNotAvailable();

    void required_eventShouldBeConsumedByProjectionWhenEverythingIsAlright();
    void required_eventShouldBeConsumedByProjectionEvenIfBrokerIsDownAtFirst();

    void required_eventShouldBeConsumedByConsistentProjectionWhenEverythingIsAlright();
    void required_eventShouldBeConsumedByConsistentProjectionEvenIfBrokerIsDownAtFirst();
    List<EventEnvelope<Evt, Meta, Context>> readFromDataStore(EventStore<TxCtx, TestEvent, Tuple0, Tuple0> eventStore);

    default void cleanup(
            EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor
    ) {
        // Default implementation is NOOP
    }
}
