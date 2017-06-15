package test.demo.es;

import kotlin.Unit;
import kotlin.jvm.JvmClassMappingKt;

import java.util.UUID;

public class TestGame {

    public static void main(String[] args) {
//        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "TRACE");


        EventStore eventStore = new EventStoreInMemory();
        DomainStore domainStore = new DomainStoreCommandAware(new DomainStoreSnapshot(eventStore, 100));
        CommandDispatcher commandDispatcher = new CommandDispatcher(eventStore);

        commandDispatcher.registerHandler(JvmClassMappingKt.getKotlinClass(CreateGameCommand.class), new CommandHandler<CreateGameCommand, UUID>() {
            @Override
            public UUID handleCommand(CreateGameCommand command) {
            	return domainStore.add(new GameAggregate(UUID.randomUUID())).getAggregateId();
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

        long startTime = System.currentTimeMillis();
        
        for (int i=0; i < 1_000_000; i++) {
            commandDispatcher.invokeCommandWithResult(new StartGameCommand(gameId));
            commandDispatcher.invokeCommandWithResult(new FinishGameCommand(gameId));
        }

        System.out.println(System.currentTimeMillis() - startTime);
    }

}
