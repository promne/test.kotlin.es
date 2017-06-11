package test.demo.es;


import org.jetbrains.annotations.NotNull;

import java.util.UUID;

public class GameFinishedEvent extends Event {
    public GameFinishedEvent(@NotNull UUID aggregateId) {
        super(aggregateId);
    }
}
