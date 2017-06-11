package test.demo.es;

import org.jetbrains.annotations.NotNull;

import java.util.UUID;

public class GameCreatedEvent extends Event {
    public GameCreatedEvent(@NotNull UUID aggregateId) {
        super(aggregateId);
    }

}
