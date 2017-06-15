import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import mu.KotlinLogging
import org.ektorp.CouchDbConnector
import org.ektorp.http.StdHttpClient
import org.ektorp.impl.StdCouchDbConnector
import org.ektorp.impl.StdCouchDbInstance
import org.ektorp.support.CouchDbRepositorySupport
import org.ektorp.support.GenerateView
import test.demo.es.Event
import test.demo.es.EventStore
import test.demo.es.StoredEvent
import test.demo.es.Timestamp
import java.util.UUID
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonInclude.Include
import org.ektorp.ViewQuery
import org.ektorp.support.CouchDbDocument
import org.ektorp.support.View

class EventStoreCouchDB(val db : CouchDbConnector) : EventStore {

	data class CouchDbStoredEvent(
		@JsonProperty("aggregateId") val aggregateId: UUID,
		@JsonProperty("timestamp") val timestamp: Timestamp,
		@JsonProperty("events") val events: List<Event>
	) : CouchDbDocument()
				
	class MyRepo(db : CouchDbConnector) : CouchDbRepositorySupport<CouchDbStoredEvent>(CouchDbStoredEvent::class.java, db) {
		
		init {
			this.initStandardDesignDocument()
		}
		
		protected fun queryView(viewName: String, key: String, skip: Int) : List<CouchDbStoredEvent> {
			return db.queryView(createQuery(viewName)
								.includeDocs(true)
								.key(key)
								.skip(skip),
							type);
		}
		
		@View(name="by_aggregateId", map = "function(doc) {emit(doc.aggregateId, doc)}")
	    fun findByAggregateId(aggregateId: UUID, skip: Int) : List<CouchDbStoredEvent> {
	        return queryView("by_aggregateId", aggregateId.toString(), skip);
	    }
	} 
	
	val repository = MyRepo(db)
	
	private val logger = KotlinLogging.logger {}
	
	companion object {
		fun connectToDb(url: String, dbName: String) : EventStoreCouchDB {
			val httpClient = StdHttpClient.Builder().url(url).build()
			val dbInstance = StdCouchDbInstance(httpClient)
			val dbConnector = StdCouchDbConnector(dbName, dbInstance)
			dbConnector.createDatabaseIfNotExists()
			return EventStoreCouchDB(dbConnector)
		}
	}
	
	override fun getEvents(aggregateId: UUID, fromVersion: Int): List<StoredEvent> {
		return repository.findByAggregateId(aggregateId, fromVersion).map { StoredEvent(it.aggregateId, it.timestamp, it.events) }
	}

	override fun store(allEvents: List<Event>) {
        allEvents.groupingBy { it.aggregateId }.fold(listOf<Event>()){ l,e -> l + e }.forEach { aggregateEvents ->
            CouchDbStoredEvent(aggregateEvents.key, Timestamp(), aggregateEvents.value).let {
                logger.trace { "Storing event $it" }
				repository.add(it)
            }
        }
	}

}