package test.demo.es;

import java.util.UUID;

public class StartGameCommand {

    private final UUID aggregateId;

    public StartGameCommand(UUID aggregateId) {
        this.aggregateId = aggregateId;
    }

    public UUID getAggregateId() {
        return aggregateId;
    }
}
