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

class EventStoreCouchDB(val db : CouchDbConnector) : EventStore {

	@JsonInclude(Include.NON_NULL)
	class CouchDbStoredEvent() {
		
		@JsonProperty("_id")
		var id: String? = null 

		@JsonProperty("_rev")
		var revision: String? = null
		 
		var aggregateId: UUID? = null
		var timestamp: Timestamp? = null
		var events: List<Event>? = null
				
		constructor(aggregateId: UUID, timestamp: Timestamp, events: List<Event>) : this() {
			this.aggregateId = aggregateId
			this.timestamp = timestamp
			this.events = events
		}
		
	}
	
	class MyRepo(val db : CouchDbConnector) : CouchDbRepositorySupport<CouchDbStoredEvent>(CouchDbStoredEvent::class.java, db) {
		
		@GenerateView
	    fun findByAggregateId(aggregateId: UUID) : List<CouchDbStoredEvent> {
	        return queryView("by_aggregateId", aggregateId.toString());
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
		logger.info { repository.all }
		logger.info { repository.findByAggregateId(aggregateId) }
		
		TODO()
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