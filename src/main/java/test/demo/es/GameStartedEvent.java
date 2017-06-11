package test.demo.es;

import org.jetbrains.annotations.NotNull;

import java.util.UUID;


public class GameStartedEvent extends Event {
    public GameStartedEvent(@NotNull UUID aggregateId) {
        super(aggregateId);
    }
}
