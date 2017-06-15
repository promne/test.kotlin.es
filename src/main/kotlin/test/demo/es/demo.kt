package test.demo.es

import mu.KotlinLogging
import java.lang.IllegalArgumentException
import java.util.*
import java.util.concurrent.Future
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorCompletionService
import EventStoreCouchDB

//////////////////////////////
// Show time
//////////////////////////////

class CounterCreatedEvent(id: UUID) : Event(id)
class CounterIncreasedEvent(id: UUID, val newValue: Long) : Event(id)
class CounterResetEvent(id: UUID) : Event(id)
class CounterLimitSetEvent(id: UUID, val limit: Long) : Event(id)

class CounterAggregate() : Aggregate() {

    var counter: Long = 0
    		private set

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
data class PrintCounterStatsCommand(val id: UUID)

fun main(args: Array<String>) {
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "TRACE")
    System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "HH:mm:ss.SSS")
    System.setProperty("org.slf4j.simpleLogger.showDateTime", "true")

    val logger = KotlinLogging.logger {}
	
    // create components
//    val eventStore = EventStoreInMemory()
	val eventStore = EventStoreCouchDB.connectToDb("http://192.168.56.101:5984","eventStoreKotlin")
	
	val domainStoreSimple: DomainStore = DomainStoreSimple(eventStore)
	val domainStoreSnapshot: DomainStore = DomainStoreSnapshot(eventStore)
    val domainStore: DomainStore = DomainStoreCommandAware(domainStoreSnapshot)
    val commandDispatcher = CommandDispatcher(eventStore)

    // register command handlers
    commandDispatcher.registerHandler(CreateCounterCommand::class) {
		domainStore.add(CounterAggregate(it.id)).aggregateId
	}
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
        counterAggregate.counter
    }
    commandDispatcher.registerHandler(PrintCounterStatsCommand::class) {		
    	val counterAggregate = domainStore.getById(CounterAggregate::class, it.id)
		logger.info { "Aggregate ${counterAggregate.aggregateId} value: ${counterAggregate.counter} from domainStore: ${domainStoreSnapshot.getById(CounterAggregate::class, it.id).counter} with events count: ${eventStore.getEvents(it.id, 0).size}" }				
    }

    // issue few commands
	val aggregateCount = 1
    val tasksCount = 1
    val iterationsPerTask = 1
	val executor = Executors.newFixedThreadPool(8)
	val completionService = ExecutorCompletionService<Unit>(executor);

	
	val counterIds : List<UUID> = (1..aggregateCount).asIterable()
			.map{UUID.randomUUID()}
			.map(::CreateCounterCommand)
			.map{ commandDispatcher.invokeCommandWithResult<UUID>(it) }		
	
    (1..tasksCount).forEach{
		completionService.submit {
			val taskStartTime = System.currentTimeMillis()
			(1..iterationsPerTask).forEach{
				counterIds.map { cid -> IncreaseCounterCommand(cid) }.forEach(commandDispatcher::invokeCommand)
			}
			val taskRunTime = System.currentTimeMillis() - taskStartTime
			counterIds.map { cid -> PrintCounterStatsCommand(cid) }.forEach(commandDispatcher::invokeCommand)
			//only the last one will be accurate - due to threaded nature
			logger.info {
				StringBuilder()
					.appendln("Stats:")	
//					.appendln("Event store size: ${eventStore.storeSize}")
					.appendln("Locks (${CommandUnitOfWork.locks.size}): ${CommandUnitOfWork.locks}")
					.appendln("Task run time $taskRunTime ms")
			}
		}
	}
	
	(1..tasksCount).forEach{
		completionService.take()
	}
	executor.shutdown()
	
	counterIds.forEach {
		logger.info { "Aggregate $it value from domainStore: ${domainStoreSimple.getById(CounterAggregate::class, it).counter} with events count: ${eventStore.getEvents(it,0).size}" }				
	}
	logger.info { "Locks: ${CommandUnitOfWork.locks}" }
	
	
	// intentionally do something wrong
	counterIds[0].let {
		SetCounterLimitCommand(it, 0).let(commandDispatcher::invokeCommand)
		IncreaseCounterCommand(it).let(commandDispatcher::invokeCommand)
	}


	
}

