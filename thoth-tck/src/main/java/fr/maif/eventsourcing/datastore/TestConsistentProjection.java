package fr.maif.eventsourcing.datastore;

import akka.actor.ActorSystem;
import fr.maif.projections.EventuallyConsistentProjection;
import io.vavr.Tuple;
import io.vavr.concurrent.Future;

import javax.sql.DataSource;

public class TestConsistentProjection {

    private final ActorSystem actorSystem;
    private final String bootstrapServer;
    private final TestEventFormat eventFormat;
    private final DataSource dataSource;
    private int counter = 0;

    public TestConsistentProjection(
            ActorSystem actorSystem,
            String bootstrapServer,
            TestEventFormat eventFormat,
            DataSource dataSource) {
        this.actorSystem = actorSystem;
        this.eventFormat = eventFormat;
        this.dataSource = dataSource;
        this.bootstrapServer = bootstrapServer;
    }


    public void init(String topic) {
        this.counter = 0;
        EventuallyConsistentProjection.create(
                ActorSystem.create(),
                "TestConsistentProjection",
                EventuallyConsistentProjection.Config.create(topic, "TestConsistentProjection", bootstrapServer),
                eventFormat,
                envelope ->
                        Future.of(() -> {
                            if (envelope.event instanceof TestEvent.SimpleEvent) {
                                counter++;
                            }
                            return Tuple.empty();
                        })

        ).start();
    }

    public int getCount() {
        return counter;
    }
}
