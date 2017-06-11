package test.demo.es

import mu.KotlinLogging
import java.lang.IllegalArgumentException
import java.util.*

//////////////////////////////
// Show time
//////////////////////////////

data class CounterCreatedEvent(val id: UUID) : Event(id)
data class CounterIncreasedEvent(val id: UUID, val newValue: Long) : Event(id)
data class CounterResetEvent(val id: UUID) : Event(id)
data class CounterLimitSetEvent(val id: UUID, val limit: Long) : Event(id)

class CounterAggregate() : Aggregate() {

    private var counter: Long = 0

    private var maximumValue: Long = Long.MAX_VALUE

    constructor (id: UUID) : this() { applyEvent(CounterCreatedEvent(id)) }

    protected fun handleCreated(event: CounterCreatedEvent) {
        aggregateId = event.aggregateId
    }

    fun increase(by: Long = 1) : CounterAggregate {
        val newCounter = counter + by
        if (newCounter <= maximumValue)
            return apply { applyEvent(CounterIncreasedEvent(aggregateId, newCounter)) }
        else
            throw IllegalArgumentException("Increase request to $newCounter is over the limit $maximumValue")
    }

    protected fun handleIncreased(event: CounterIncreasedEvent) {
        counter = event.newValue;
    }

    fun getCount() : Long = counter;

    fun  setLimit(limit: Long) {
        applyEvent(CounterLimitSetEvent(aggregateId, limit))
    }

    protected fun handleLimitSet(event: CounterLimitSetEvent) {
        maximumValue = event.limit
    }

    fun resetCounter() {
        applyEvent(CounterResetEvent(aggregateId))
    }

    protected fun handleReset(event: CounterResetEvent) {
        counter = 0
    }
}

data class CreateCounterCommand(val id: UUID)
data class SetCounterLimitCommand(val id: UUID, val limit: Long)
data class IncreaseCounterCommand(val id: UUID, val by: Long = 1)
data class ResetCounterAndLimitCommand(val id: UUID)

fun main(args: Array<String>) {
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "TRACE")

    val logger = KotlinLogging.logger {}

    // create components
    val eventStore : EventStore = EventStore()
    val domainStore: DomainStore = DomainStore(eventStore)
    val commandDispatcher = CommandDispatcher(eventStore)

    // register command handlers
    commandDispatcher.registerHandler(CreateCounterCommand::class) { CounterAggregate(it.id).aggregateId }
    commandDispatcher.registerHandler(SetCounterLimitCommand::class) { domainStore.getById(CounterAggregate::class, it.id).setLimit(it.limit) }
    commandDispatcher.registerHandler(ResetCounterAndLimitCommand::class) { command ->
        domainStore.getById(CounterAggregate::class, command.id).let {
            it.setLimit(Long.MAX_VALUE)
            it.resetCounter()
        }
    }
    commandDispatcher.registerHandler(IncreaseCounterCommand::class) {
        val counterAggregate = domainStore.getById(CounterAggregate::class, it.id)
        counterAggregate.increase(it.by)
        counterAggregate.getCount()
    }

    // issue few commands
    val newId : UUID = commandDispatcher.invokeCommandWithResult(CreateCounterCommand(UUID.randomUUID())) // calling this command expects result
    commandDispatcher.invokeCommand(SetCounterLimitCommand(newId, 60))

    logger.info { "New aggregate created with id $newId and count ${domainStore.getById(CounterAggregate::class, newId).getCount()}" }

    (1..5).forEach {
        // we don't need to care about internal logic (limit of the aggregate in this case)
        val commandResult: Long = commandDispatcher.invokeCommandWithResult(IncreaseCounterCommand(newId, it.toLong()))
        logger.info {"Result of command increasing $newId by $it : $commandResult"}
        logger.info { "Aggregate $newId value from domainStore: ${domainStore.getById(CounterAggregate::class, newId).getCount()}" }
    }

    commandDispatcher.invokeCommand(ResetCounterAndLimitCommand(newId))

    // intentionally do something wrong
    commandDispatcher.invokeCommand(SetCounterLimitCommand(newId, 0))
    eventStore.events.forEach { logger.info {it} }
    commandDispatcher.invokeCommand(IncreaseCounterCommand(newId))


}

