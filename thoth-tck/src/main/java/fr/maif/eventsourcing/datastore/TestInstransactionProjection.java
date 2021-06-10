package fr.maif.eventsourcing.datastore;

import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.Projection;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.concurrent.Future;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class TestInstransactionProjection implements Projection<Connection, TestEvent, Tuple0, Tuple0> {


    @Override
    public Future<Tuple0> storeProjection(Connection connection, List<EventEnvelope<TestEvent, Tuple0, Tuple0>> envelopes) {
        return Future.of(() -> {
            try (PreparedStatement incrementStatement = connection.prepareStatement("UPDATE test_projection SET counter=counter+1")) {
                for (EventEnvelope<TestEvent, Tuple0, Tuple0> envelope : envelopes) {
                    if (envelope.event instanceof TestEvent.SimpleEvent) {
                        incrementStatement.addBatch();
                        incrementStatement.executeBatch();
                    }
                }
                return Tuple.empty();
            } catch (SQLException ex) {
                throw new RuntimeException("Failed to update projection", ex);
            }

        });


    }


}
