package fr.maif.eventsourcing.datastore;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.Type;
import io.vavr.API;

public abstract class TestEvent implements Event {
    public final String id;

    public static Type<SimpleEvent> SimpleEventV1 = Type.create(SimpleEvent.class, 1L);
    public static Type<DeleteEvent> DeleteEventV1 = Type.create(DeleteEvent.class, 1L);
    static API.Match.Pattern0<SimpleEvent> $SimpleEvent() {
        return API.Match.Pattern0.of(SimpleEvent.class);
    }
    @Override
    public String entityId() {
        return id;
    }

    public TestEvent(String id) {
        this.id = id;
    }

    public static class SimpleEvent extends TestEvent {

        @JsonCreator
        public SimpleEvent(@JsonProperty("id") String id) {
            super(id);
        }

        @Override
        public Type<?> type() {
            return SimpleEventV1;
        }
    }

    public static class DeleteEvent extends TestEvent {

        @JsonCreator
        public DeleteEvent(@JsonProperty("id") String id) {
            super(id);
        }

        @Override
        public Type<?> type() {
            return DeleteEventV1;
        }
    }
}
