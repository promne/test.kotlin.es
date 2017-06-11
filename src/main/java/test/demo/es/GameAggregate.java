package test.demo.es;

import java.util.UUID;

public class GameAggregate extends Aggregate {

    private boolean gameInProgress = false;

    public GameAggregate() {
        // for reflection
    }

    public GameAggregate(UUID id) {
        applyEvent(new GameCreatedEvent(id));
    }

    protected void handleCreated(GameCreatedEvent e) {
        this.setAggregateId(e.getAggregateId());
    }

    public void start() {
        if (gameInProgress) {
            throw new IllegalStateException("Game is already started");
        }
        applyEvent(new GameStartedEvent(this.getAggregateId()));
    }

    protected void handleStarted(GameStartedEvent e) {
        this.gameInProgress = true;
    }

    public void finish() {
        if (!gameInProgress) {
            throw new IllegalStateException("Game is not running");
        }
        applyEvent(new GameFinishedEvent(this.getAggregateId()));
    }

    protected void handleFinished(GameFinishedEvent e) {
        this.gameInProgress = false;
    }

}
