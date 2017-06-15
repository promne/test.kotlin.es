package test.demo.es

import mu.KotlinLogging
import java.util.UUID
import kotlin.reflect.KClass
import java.lang.IllegalArgumentException
import java.lang.reflect.Method
import java.util.Collections
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.TimeUnit
import java.lang.reflect.Type

/////////////////
/// Event sourcing
/////////////////
open class Event(val aggregateId: UUID)


abstract class Aggregate() {
	
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

data class StoredEvent(val aggregateId: UUID, val timestamp: Timestamp, val events: List<Event>)

interface EventStore {
	
	fun getEvents(aggregateId: UUID, fromVersion: Int) : List<StoredEvent>
	
	fun store(allEvents: List<Event>)
		
}

class EventStoreInMemory : EventStore {
    private val logger = KotlinLogging.logger {}

    //in memory
	private val events: MutableMap<UUID, MutableList<StoredEvent>> = ConcurrentHashMap()
	
	private fun getAggregateEventsList(aggregateId: UUID) : MutableList<StoredEvent> = events.computeIfAbsent(aggregateId) { Collections.synchronizedList(ArrayList()) }
	
	override fun getEvents(aggregateId: UUID, fromVersion: Int) : List<StoredEvent> = getAggregateEventsList(aggregateId).drop(fromVersion)
	
	override fun store(allEvents: List<Event>) {
        allEvents.groupingBy { it.aggregateId }.fold(listOf<Event>()){ l,e -> l + e }.forEach { aggregateEvents ->
            StoredEvent(aggregateEvents.key, Timestamp(), aggregateEvents.value).let {
                logger.trace { "Storing event $it" }
                getAggregateEventsList(it.aggregateId).add(it);
            }
        }

	}
	
	val storeSize : Long
			get() = events.values.map{ it.size }.fold(0, Long::plus)
	
}

interface DomainStore {
	fun <T : Aggregate> getById(aggregateClass: KClass<T>, aggregateId: UUID) : T
	fun <T : Aggregate> add(aggregate : T) : T	
}

class DomainStoreSimple(val eventStore: EventStore) : DomainStore {

	override fun <T : Aggregate> getById(aggregateClass: KClass<T>, aggregateId: UUID) : T {	
		val events = eventStore.getEvents(aggregateId, 0)
		if (events.isNotEmpty()) {
			val newAggregate: T = aggregateClass.java.newInstance()
			events.map { it.events }.flatMap { it }.forEach(newAggregate::handleEvent)
			return newAggregate
		}
		throw IllegalArgumentException("Aggregate $aggregateId not found")
	}

	override fun <T : Aggregate> add(aggregate : T) : T {
		eventStore.store(aggregate.newEvents)
		return aggregate
	}	
	
}

class DomainStoreSnapshot(val eventStore: EventStore, val versionsToSnapshot : Int = 100) : DomainStore {
	
	data class Snapshot(val aggregate: Aggregate, val aggregateVersion: Int)
	
	val snapshotStore: MutableMap<UUID, Snapshot> = mutableMapOf()
	
	@Suppress("UNCHECKED_CAST")
	override fun <T : Aggregate> getById(aggregateClass: KClass<T>, aggregateId: UUID) : T {
		val snapshot : Snapshot? = snapshotStore[aggregateId]
		val fromVersion: Int = snapshot?.aggregateVersion ?: 0
		
		val events = eventStore.getEvents(aggregateId, fromVersion)		
		if (events.isNotEmpty() or (fromVersion>0)) {
			val aggregate: T = (snapshot?.aggregate ?: aggregateClass.java.newInstance()) as T
			events.map { it.events }.flatMap { it }.forEach(aggregate::handleEvent)
			
			if (events.size==versionsToSnapshot) {
				snapshotStore[aggregateId] = Snapshot(aggregate, fromVersion + versionsToSnapshot)
			}
			aggregate.newEvents.clear()
			return aggregate
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
						
		val locks: MutableMap<UUID, ReentrantLock> = ConcurrentHashMap()
	}
			
	class CommandUnitOfWorkData() {
		private val logger = KotlinLogging.logger {}
		
		val aggregates : MutableMap<UUID, Aggregate> = mutableMapOf()
		
		val lockIds: MutableList<UUID> = mutableListOf()
		
		val events : List<Event>
			get() = aggregates.values.flatMap { it.newEvents }
		
		fun clear() {
			lockIds.forEach { aggregateId ->
				locks.get(aggregateId)?.let { lock ->
					while (lock.isHeldByCurrentThread) lock.unlock()
					logger.trace {"Released lock for aggregate $aggregateId"}
				}
			}
			Holder.INSTANCE.remove()
		}
		
	}	
			
	
}

class DomainStoreCommandAware(val wrappedDomainStore: DomainStore) : DomainStore {
	
    private val logger = KotlinLogging.logger {}	

	@Suppress("UNCHECKED_CAST")
	override fun <T : Aggregate> getById(aggregateClass: KClass<T>, aggregateId: UUID) : T {
		val lock = CommandUnitOfWork.locks.computeIfAbsent(aggregateId) {ReentrantLock()}
		lock.lock()
		logger.trace { "Acquired lock $lock for aggregate $aggregateId with hold count ${lock.holdCount}" }
		CommandUnitOfWork.current.lockIds.add(aggregateId)
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

	@Suppress("UNCHECKED_CAST")
	fun <R: Any> invokeCommandWithResult(command: Any) : R {
        CommandUnitOfWork.current.clear()
        try {
            val handler = handlers.getOrElse(command::class) {throw IllegalArgumentException("Command handler is not registered for $command") } as CommandHandler<Any, R>
            logger.debug { "Handling command $command" }
            val commandResult = handler.handleCommand(command)
            logger.debug { "Command $command generated events ${CommandUnitOfWork.current.events}" }
            CommandUnitOfWork.current.events.let(eventStore::store)
            return commandResult
		} catch (e: Exception) {
			logger.error { e }
			throw e
        } finally {
            CommandUnitOfWork.current.clear()
        }
	}

	fun invokeCommand(command: Any) {
		invokeCommandWithResult<Any>(command)
	}
		
}

