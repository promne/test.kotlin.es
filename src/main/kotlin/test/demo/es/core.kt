package test.demo.es

import mu.KotlinLogging
import java.util.UUID
import kotlin.reflect.KClass
import java.lang.IllegalArgumentException
import java.lang.reflect.Method
import java.util.Collections
import java.util.concurrent.CopyOnWriteArrayList

/////////////////
/// Event sourcing
/////////////////
open class Event(val aggregateId: UUID)


open class Aggregate() {
	
	var aggregateId = UUID.nameUUIDFromBytes(ByteArray(0))
			protected set
	
	val eventHandlerMethodCache : MutableMap<Class<Event>, Method> = mutableMapOf()
	fun handleEvent(event: Event) {
		eventHandlerMethodCache.getOrPut(event.javaClass) { this.javaClass.declaredMethods.single { it.parameterCount==1 && it.parameterTypes[0]==event.javaClass }}.invoke(this, event)
	}
	
	val newEvents : MutableList<Event> = mutableListOf()
	fun applyEvent(event: Event) {
		newEvents.add(event)
		this.handleEvent(event)
	}

}


data class Timestamp(val i: Long = System.nanoTime())

data class StoredEvent(val aggregateId: UUID, val timestamp: Timestamp, val event: List<Event>)

class EventStore {
    private val logger = KotlinLogging.logger {}

    //in memory
	val events: MutableList<StoredEvent> = Collections.synchronizedList(CopyOnWriteArrayList())
	
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

interface DomainStore {
	fun <T : Aggregate> getById(aggregateClass: KClass<T>, aggregateId: UUID) : T
	fun <T : Aggregate> add(aggregate : T) : T	
}

class DomainStoreSimple(val eventStore: EventStore) : DomainStore {

	override fun <T : Aggregate> getById(aggregateClass: KClass<T>, aggregateId: UUID) : T {	
		val events = eventStore.getEvents(aggregateId)
		if (events.isNotEmpty()) {
			val newAggregate: T = aggregateClass.java.newInstance()
			events.map { it.event }.flatMap { it }.forEach(newAggregate::handleEvent)
			return newAggregate
		}
		throw IllegalArgumentException("Aggregate $aggregateId not found")
	}

	override fun <T : Aggregate> add(aggregate : T) : T {
		eventStore.store(aggregate.newEvents)
		return aggregate
	}	
	
}

///////////////////////
// CQRS - Command part
///////////////////////

// mix in threads
class CommandUnitOfWork private constructor() {
	
	private object Holder {
		val INSTANCE = ThreadLocal.withInitial {CommandUnitOfWorkData()}
	}
	
	companion object {
		val current : CommandUnitOfWorkData
			get() = Holder.INSTANCE.get()				
	}
			
	class CommandUnitOfWorkData() {
		
		val aggregates : MutableMap<UUID, Aggregate> = mutableMapOf()
		
		val events : List<Event>
			get() = aggregates.values.flatMap { it.newEvents }
		
		fun clear() {
			Holder.INSTANCE.remove()
		}
		
	}	
			
	
}

class DomainStoreCommandAware(val wrappedDomainStore: DomainStore) : DomainStore {

	@Suppress("UNCHECKED_CAST")
	override fun <T : Aggregate> getById(aggregateClass: KClass<T>, aggregateId: UUID) : T {
		return CommandUnitOfWork.current.aggregates.getOrPut(aggregateId) {wrappedDomainStore.getById(aggregateClass, aggregateId)} as T
	}

	override fun <T : Aggregate> add(aggregate : T) : T {
		CommandUnitOfWork.current.aggregates.put(aggregate.aggregateId, aggregate)
		return aggregate
	}
	
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
        CommandUnitOfWork.current.clear()
        try {
            val handler = handlers.getOrElse(command::class) {throw IllegalArgumentException("Command handler is not registered for $command") } as CommandHandler<Any, R>
            logger.debug { "Handling command $command" }
            val commandResult = handler.handleCommand(command)
            logger.debug { "Command $command generated events ${CommandUnitOfWork.current.events}" }
            CommandUnitOfWork.current.events.let(eventStore::store)
            return commandResult
        } finally {
            CommandUnitOfWork.current.clear()
        }
	}

	fun invokeCommand(command: Any) {
		invokeCommandWithResult<Any>(command)
	}
		
}

