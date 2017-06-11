package test.demo.es;

import kotlin.Unit;
import kotlin.jvm.JvmClassMappingKt;

import java.util.UUID;

public class TestGame {

    public static void main(String[] args) {
//        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "TRACE");

        long startTime = System.nanoTime();

        EventStore eventStore = new EventStore();
        DomainStore domainStore = new DomainStore(eventStore);
        CommandDispatcher commandDispatcher = new CommandDispatcher(eventStore);

        commandDispatcher.registerHandler(JvmClassMappingKt.getKotlinClass(CreateGameCommand.class), new CommandHandler<CreateGameCommand, UUID>() {
            @Override
            public UUID handleCommand(CreateGameCommand command) {
                GameAggregate game = new GameAggregate(UUID.randomUUID());
                return game.getAggregateId();
            }
        });
        commandDispatcher.registerHandler(JvmClassMappingKt.getKotlinClass(StartGameCommand.class), new CommandHandler<StartGameCommand, Unit>() {
            @Override
            public Unit handleCommand(StartGameCommand command) {
                domainStore.getById(JvmClassMappingKt.getKotlinClass(GameAggregate.class), command.getAggregateId()).start();
                return Unit.INSTANCE;
            }
        });
        commandDispatcher.registerHandler(JvmClassMappingKt.getKotlinClass(FinishGameCommand.class), new CommandHandler<FinishGameCommand, Unit>() {
            @Override
            public Unit handleCommand(FinishGameCommand command) {
                domainStore.getById(JvmClassMappingKt.getKotlinClass(GameAggregate.class), command.getAggregateId()).finish();
                return Unit.INSTANCE;
            }
        });


        UUID gameId = commandDispatcher.invokeCommandWithResult(new CreateGameCommand());
        System.out.println(gameId);

        for (int i=0; i<5400; i++) {
            commandDispatcher.invokeCommandWithResult(new StartGameCommand(gameId));
            commandDispatcher.invokeCommandWithResult(new FinishGameCommand(gameId));
        }

    }

}
