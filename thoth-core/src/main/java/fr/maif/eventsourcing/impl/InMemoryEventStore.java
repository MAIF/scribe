package fr.maif.eventsourcing.impl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.Materializer;
import akka.stream.OverflowStrategy;
import akka.stream.javadsl.*;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.EventStore;
import io.vavr.collection.List;
import io.vavr.concurrent.Future;
import io.vavr.control.Option;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class InMemoryEventStore<E extends Event, Meta, Context> implements EventStore<Tuple0, E, Meta, Context> {

    private final ActorSystem system;
    private final Materializer materializer;

    private java.util.List<EventEnvelope<E, Meta, Context>> eventStore = new ArrayList<>();

    private final SourceQueueWithComplete<EventEnvelope> queue;
    private final Source<EventEnvelope, NotUsed> realTimeEvents;

    private AtomicLong sequence_num = new AtomicLong(0);

    private final ConcurrentHashMap<String, Long> offsets = new ConcurrentHashMap<>();

    private InMemoryEventStore(ActorSystem system) {
        this.system = system;
        this.materializer = Materializer.createMaterializer(system);

        Pair<SourceQueueWithComplete<EventEnvelope>, Source<EventEnvelope, NotUsed>> run = Source
                .<EventEnvelope>queue(500, OverflowStrategy.backpressure())
                .toMat(BroadcastHub.of(EventEnvelope.class, 256), Keep.both())
                .run(materializer);

        this.queue = run.first();
        this.realTimeEvents = run.second();
        this.realTimeEvents.runWith(Sink.ignore(), materializer);
    }

    public static <E extends Event, Meta, Context> InMemoryEventStore<E, Meta, Context> create(ActorSystem system) {
        return new InMemoryEventStore<>(system);
    }

    @Override
    public Source<EventEnvelope<E, Meta, Context>, NotUsed> loadEventsUnpublished(Tuple0 tx, ConcurrentReplayStrategy concurrentReplayStrategy) {
        return Source.empty();
    }

    @Override
    public Future<EventEnvelope<E, Meta, Context>> markAsPublished(Tuple0 tx, EventEnvelope<E, Meta, Context> eventEnvelope) {
        return markAsPublished(eventEnvelope);
    }

    @Override
    public Future<Tuple0> openTransaction() {
        return Future.successful(Tuple.empty());
    }

    @Override
    public Future<Tuple0> commitOrRollback(Option<Throwable> of, Tuple0 tx) {
        return Future.successful(Tuple.empty());
    }

    @Override
    public Future<EventEnvelope<E, Meta, Context>> markAsPublished(EventEnvelope<E, Meta, Context> eventEnvelope) {
        return Future.successful(
                eventEnvelope.copy().withPublished(true).build()
        );
    }

    @Override
    public ActorSystem system() {
        return this.system;
    }

    @Override
    public Materializer materializer() {
        return this.materializer;
    }

    @Override
    public Future<Long> nextSequence(Tuple0 tx) {
        return Future.successful(sequence_num.incrementAndGet());
    }

    @Override
    public Future<Tuple0> publish(List<EventEnvelope<E, Meta, Context>> events) {
        events.forEach(queue::offer);
        return Future.successful(Tuple.empty());
    }

    @Override
    public Source<EventEnvelope<E, Meta, Context>, NotUsed> loadEvents(String id) {
        return Source.from(eventStore);
    }


    @Override
    public Source<EventEnvelope<E, Meta, Context>, NotUsed> loadAllEvents() {
        return Source.from(eventStore);
    }

    @Override
    public Source<EventEnvelope<E, Meta, Context>, NotUsed> loadEventsByQuery(Tuple0 tx, Query query) {
        return loadEventsByQuery(query);
    }

    @Override
    public Source<EventEnvelope<E, Meta, Context>, NotUsed> loadEventsByQuery(Query query) {
        return Source.from(eventStore)
                .filter(e -> Option.of(query.entityId).map(id -> id.equals(e.entityId)).getOrElse(true));
    }

    @Override
    public Future<Tuple0> persist(Tuple0 transactionContext, List<EventEnvelope<E, Meta, Context>> events) {
        eventStore.addAll(events.toJavaList());
        return Future.successful(Tuple.empty());
    }
}
