package fr.maif.eventsourcing.datastore;
import fr.maif.jooq.PgAsyncTransaction;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.Projection;
import io.vavr.API;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.concurrent.Future;

import static io.vavr.API.Case;
import static io.vavr.API.Match;
import static io.vavr.PartialFunction.unlift;


public class TestInstransactionProjectionAsync implements Projection<PgAsyncTransaction, TestEvent, Tuple0, Tuple0> {

    @Override
    public Future<Tuple0> storeProjection(PgAsyncTransaction connection, List<EventEnvelope<TestEvent, Tuple0, Tuple0>> envelopes) {
        return connection.executeBatch(dsl ->
                envelopes.collect(unlift(eventEnvelope ->
                                Match(eventEnvelope.event).option(
                                        Case(TestEvent.$SimpleEvent(), e -> API.Tuple(eventEnvelope, e))
                                )))
                        .map(t -> dsl.query("UPDATE test_projection SET counter=counter+1" ))
        ).map(__ -> Tuple.empty());
    }




}
