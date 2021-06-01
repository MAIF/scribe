package fr.maif.eventsourcing.datastore;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import fr.maif.eventsourcing.Projection;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;

import akka.actor.ActorSystem;
import akka.stream.javadsl.Sink;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.EventProcessor;
import fr.maif.eventsourcing.EventStore;
import fr.maif.eventsourcing.TransactionManager;
import fr.maif.eventsourcing.impl.InMemoryEventStore;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.concurrent.Future;

public class InMemoryDataStoreTest extends DataStoreVerification<Tuple0> {
    public InMemoryEventStore<TestEvent, Tuple0, Tuple0> eventStore;
    public EventProcessor<String, TestState, TestCommand, TestEvent, Tuple0, Tuple0, Tuple0, Tuple0> eventProcessor;


    @BeforeMethod(alwaysRun = true)
    public void init() {
        this.eventStore = Mockito.spy(InMemoryEventStore.create(actorSystem));
        this.eventProcessor = new EventProcessor<>(
                actorSystem,
                eventStore,
                noOpTransactionManager(),
                new TestCommandHandler(),
                new TestEventHandler(),
                io.vavr.collection.List.empty()
        );
    }

    @Override
    public List<EventEnvelope<TestEvent, Tuple0, Tuple0>> readPublishedEvents(String kafkaBootStrapUrl, String topic) {
        try {
            return this.eventStore.loadAllEvents().runWith(Sink.seq(), actorSystem).toCompletableFuture().get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer readProjection() {
        // Not implemented for in memory
        return null;
    }

    @Override
    public void required_eventShouldBeConsumedByProjectionWhenEverythingIsAlright(){
        // Not implemented for in memory
    }
    @Override
    public void required_eventShouldBeConsumedByProjectionEvenIfBrokerIsDownAtFirst(){
        // Not implemented for in memory
    }
    @Override
    public void required_eventShouldNotBeConsumedByProjectionEvenIfDataBaseIsBroken(){
        // Not implemented for in memory
    }
    @Override
    public void required_commandSubmissionShouldFailIfDatabaseIsNotAvailable() {
        // Not implemented for in memory
    }


    @Override
    public void required_eventShouldBePublishedEventIfBrokerIsDownAtFirst() {
        // Not implemented for in memory
    }

    @Override
    public void shutdownBroker() {
        throw new RuntimeException("Not implemented for in memory");
    }

    @Override
    public void restartBroker() {
        Mockito.reset(eventStore);
        throw new RuntimeException("Not implemented for in memory");
    }

    @Override
    public void shutdownDatabase() {
        throw new RuntimeException("Not implemented for in memory");
    }

    @Override
    public void restartDatabase() {
        throw new RuntimeException("Not implemented for in memory");
    }

    @Override
    public EventProcessor<String, TestState, TestCommand, TestEvent, Tuple0, Tuple0, Tuple0, Tuple0> eventProcessor(String topic) {
        return this.eventProcessor;
    }

    @Override
    public String kafkaBootstrapUrl() {
        return null;
    }

    private TransactionManager<Tuple0> noOpTransactionManager() {
        return new TransactionManager<Tuple0>() {
            @Override
            public <T> Future<T> withTransaction(Function<Tuple0, Future<T>> function) {
                return function.apply(Tuple.empty());
            }
        };
    }
}
