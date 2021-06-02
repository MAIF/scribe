package fr.maif.eventsourcing.datastore;

import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.Projection;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.concurrent.Future;

import java.sql.Connection;

public class TestInstransactionProjection implements Projection<Connection, TestEvent, Tuple0, Tuple0> {
    private int counter = 0;

    @Override
    public Future<Tuple0> storeProjection(Connection connection, List<EventEnvelope<TestEvent, Tuple0, Tuple0>> envelopes) {
        return Future.of(() -> {
            envelopes.forEach(envelope -> {
                if (envelope.event instanceof TestEvent.SimpleEvent) {
                    counter++;
                }
            });
            return Tuple.empty();
        });
    }

    public int getCount() {
        return counter;
    }


}
