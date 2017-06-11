package test.demo.es;

import org.jetbrains.annotations.NotNull;

import java.util.UUID;

public class FinishGameCommand {
    private final UUID aggregateId;

    FinishGameCommand(@NotNull UUID aggregateId) {
        this.aggregateId = aggregateId;
    }

    public UUID getAggregateId() {
        return aggregateId;
    }
}
