package test.demo.es

import mu.KotlinLogging
import java.util.UUID
import kotlin.reflect.KClass
import java.lang.IllegalArgumentException

/////////////////
/// Event sourcing
/////////////////
open class Event(val aggregateId: UUID)

open class Aggregate() {
	var aggregateId: UUID = UUID.nameUUIDFromBytes(ByteArray(0))
	
	fun handleEvent(event: Event) {
        // I know, this is slow. Cache results/introduce event handlers bound to aggregate & event
		this.javaClass.declaredMethods.single { it.parameterCount==1 && it.parameterTypes[0]==event.javaClass }.invoke(this, event)
	}	
}


data class Timestamp(val i: Long = System.nanoTime())

data class StoredEvent(val aggregateId: UUID, val timestamp: Timestamp, val event: Event)

class EventStore {
    private val logger = KotlinLogging.logger {}

    //in memory
	val events: MutableList<StoredEvent> = mutableListOf();
	
	fun getEvents(aggregateId: UUID) : List<StoredEvent> = events.filter { it.aggregateId==aggregateId }
	
	fun store(event: Event) {
        StoredEvent(event.aggregateId, Timestamp(), event).let {
            logger.trace { "Storing event $it" }
            events.add(it);
        }

	}
	
}

class DomainStore(val eventStore: EventStore) {
	
	fun <T : Aggregate> getById(aggregateClass: KClass<T>, aggregateId: UUID) : T {
		val events = eventStore.getEvents(aggregateId)
		if (events.isNotEmpty()) {
			val newAggregate: T = aggregateClass.java.newInstance()
			events.map { it.event }.forEach(newAggregate::handleEvent)
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

// bring command dispatch scope to the Aggregate
fun Aggregate.applyEvent(event: Event) {
    CommandEvents.applyEvent(event)
	this.handleEvent(event)
}

// Collect command handlers and dispatch command to appropriate ones
class CommandDispatcher(val eventStore: EventStore) {

    private val logger = KotlinLogging.logger {}

    interface CommandHandler<T, R> {
        fun handleCommand(command: T): R
    }

    //wrap function to object so we can store the reference
    object CommandHandlerFactory {
        fun <T, R> create(handleFunction : (T) -> R) : CommandHandler<T, R> {
            return object : CommandHandler<T, R> {
                override fun handleCommand(command: T): R = handleFunction(command)
            }
        }
    }

	val handlers = mutableMapOf<KClass<out Any>, CommandHandler<out Any, out Any>>()

	fun <T: Any, R: Any> registerHandler(commandClass: KClass<T>, handleCommandBody: (T) -> R) = registerHandler(commandClass, CommandHandlerFactory.create(handleCommandBody))

	fun <T: Any, R: Any> registerHandler(commandClass: KClass<T>, commandHandler: CommandHandler<T, R>) {
		handlers[commandClass]?.let { throw IllegalArgumentException("Command has been already registered: $commandClass") }
		handlers[commandClass] = commandHandler
        logger.info {"Command handler for $commandClass registered"}
	}

	// command can return result
	fun <R: Any> invokeCommand(command: Any) : R {
        CommandEvents.clear()
        try {
            val handler = handlers.getOrElse(command::class) {throw IllegalArgumentException("Command handler is not registered for $command") } as CommandHandler<Any, R>
            logger.debug { "Handling command $command" }
            val commandResult = handler.handleCommand(command)
            logger.debug { "Command $command generated events ${CommandEvents.getEvents()}" }
            CommandEvents.getEvents().forEach(eventStore::store)
            return commandResult
        } finally {
            CommandEvents.clear()
        }
	}

	//but doesn't have to
	fun invokeCommand(command: Any) {
		invokeCommand<Any>(command)
	}
		
}

