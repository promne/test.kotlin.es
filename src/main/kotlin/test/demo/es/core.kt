package test.demo.es

import mu.KotlinLogging
import java.util.UUID
import kotlin.reflect.KClass
import java.lang.IllegalArgumentException
import java.lang.reflect.Method

/////////////////
/// Event sourcing
/////////////////
open class Event(val aggregateId: UUID)

open class Aggregate() {
	var aggregateId: UUID = UUID.nameUUIDFromBytes(ByteArray(0))

	val eventHandlerMethodCache : MutableMap<Class<Event>, Method> = mutableMapOf()

	fun applyEvent(event: Event) {
		CommandEvents.applyEvent(event)
		this.handleEvent(event)
	}

	fun handleEvent(event: Event) {
		eventHandlerMethodCache.getOrPut(event.javaClass) { this.javaClass.declaredMethods.single { it.parameterCount==1 && it.parameterTypes[0]==event.javaClass }}.invoke(this, event)
	}
}


data class Timestamp(val i: Long = System.nanoTime())

data class StoredEvent(val aggregateId: UUID, val timestamp: Timestamp, val event: List<Event>)

class EventStore {
    private val logger = KotlinLogging.logger {}

    //in memory
	val events: MutableList<StoredEvent> = mutableListOf();
	
	fun getEvents(aggregateId: UUID) : List<StoredEvent> = events.filter { it.aggregateId==aggregateId }
	
	fun store(allEvents: List<Event>) {
        allEvents.groupingBy { it.aggregateId }.fold(listOf<Event>()){ l,e -> l + e }.forEach { aggregateEvents ->
            StoredEvent(aggregateEvents.key, Timestamp(), aggregateEvents.value).let {
                logger.trace { "Storing event $it" }
                events.add(it);
            }
        }

	}
	
}

class DomainStore(val eventStore: EventStore) {

	fun <T : Aggregate> getById(aggregateClass: KClass<T>, aggregateId: UUID) : T {
		val events = eventStore.getEvents(aggregateId)
		if (events.isNotEmpty()) {
			val newAggregate: T = aggregateClass.java.newInstance()
			events.map { it.event }.flatMap { it }.forEach(newAggregate::handleEvent)
			return newAggregate
		}
		throw IllegalArgumentException("Aggregate $aggregateId not found")
	}

}

///////////////////////
// CQRS - Command part
///////////////////////

// mix in threads
object CommandEvents {

    class CommandEventsData(val events: MutableList<Event> = mutableListOf(), var aggregateId: UUID? = null) {
        fun addEvent(event: Event) {
            if (events.isEmpty()) aggregateId = event.aggregateId
            if (event.aggregateId != aggregateId) throw IllegalArgumentException("Can't accept another aggregate event")
            events.add(event)
        }
    }

	private val eventsData = ThreadLocal<CommandEventsData>()

	fun clear() {
        eventsData.set(CommandEventsData())
    }
	
	fun applyEvent(e: Event) {
        eventsData.get().addEvent(e)
    }

	fun getEvents() = eventsData.get().events
}


interface CommandHandler<T, R> {
	fun handleCommand(command: T): R
}

class CommandHandlerImpl<T, R>(val handleFunction : (T) -> R) : CommandHandler<T,R> {
    override fun handleCommand(command: T): R = handleFunction(command)
}

// Collect command handlers and dispatch command to appropriate ones
class CommandDispatcher(val eventStore: EventStore) {

    private val logger = KotlinLogging.logger {}

	val handlers = mutableMapOf<KClass<out Any>, CommandHandler<out Any, out Any>>()

	fun <T: Any, R: Any> registerHandler(commandClass: KClass<T>, handleCommandBody: (T) -> R) = registerHandler(commandClass, CommandHandlerImpl(handleCommandBody))

	fun <T: Any, R: Any> registerHandler(commandClass: KClass<T>, commandHandler: CommandHandler<T, R>) {
		handlers[commandClass]?.let { throw IllegalArgumentException("Command has been already registered: $commandClass") }
		handlers[commandClass] = commandHandler
        logger.info {"Command handler for $commandClass registered"}
	}

	fun <R: Any> invokeCommandWithResult(command: Any) : R {
        CommandEvents.clear()
        try {
            val handler = handlers.getOrElse(command::class) {throw IllegalArgumentException("Command handler is not registered for $command") } as CommandHandler<Any, R>
            logger.debug { "Handling command $command" }
            val commandResult = handler.handleCommand(command)
            logger.debug { "Command $command generated events ${CommandEvents.getEvents()}" }
            CommandEvents.getEvents().let(eventStore::store)
            return commandResult
        } finally {
            CommandEvents.clear()
        }
	}

	fun invokeCommand(command: Any) {
		invokeCommandWithResult<Any>(command)
	}
		
}

