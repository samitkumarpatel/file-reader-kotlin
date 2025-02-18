package net.samitkumar.filereaderkotlin

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import lombok.extern.slf4j.Slf4j
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.event.EventListener
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.data.redis.serializer.RedisSerializationContext
import org.springframework.stereotype.Component
import org.springframework.web.reactive.HandlerMapping
import org.springframework.web.reactive.function.server.RouterFunction
import org.springframework.web.reactive.function.server.RouterFunctions
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping
import org.springframework.web.reactive.socket.WebSocketHandler
import org.springframework.web.reactive.socket.WebSocketSession
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Schedulers
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

@SpringBootApplication
@Slf4j
class FileReaderKotlinApplication {

	@Value("\${spring.application.file.lookup.path}")
	lateinit var fileLookupPath: String

	@Bean
	fun sinks(): Sinks.Many<String> = Sinks.many().multicast().onBackpressureBuffer()

	@Autowired
	//autowired reactiveRedisTemplate
	lateinit var reactiveRedisTemplate: ReactiveRedisTemplate<String, String>

	@Bean
	fun routerFunction(): RouterFunction<ServerResponse> {
		return RouterFunctions.route()
			.GET("/ping"){
				ServerResponse.ok().bodyValue(mapOf("message" to "PONG"))
			}
			.GET("/details"){ request ->
				val filename = request.queryParams().getFirst("filename").toString()
				println("Received request for file: $filename")
				ServerResponse.ok().bodyValue(processFile(filename))
			}
			.build()
	}

	@Bean
	fun handlerMapping(myWebSocketHandler: MyWebSocketHandler): HandlerMapping {
		val map: MutableMap<String, WebSocketHandler> = HashMap()
		map["/ws-kotlin"] = myWebSocketHandler
		val order = -1 // before annotated controllers
		return SimpleUrlHandlerMapping(map, order)
	}

	@Bean
	fun reactiveRedisTemplate(factory: ReactiveRedisConnectionFactory): ReactiveRedisTemplate<String, String> {
		return ReactiveRedisTemplate(factory, RedisSerializationContext.string())
	}

	//Initialize reactiveRedisTemplate

	@EventListener(ApplicationReadyEvent::class)
	fun onApplicationEvent() {
		println("Redis Subscription is up and running...")
		reactiveRedisTemplate.listenToChannel("channel")
			.doOnNext { processedMessage -> println("[*] Received Message: $processedMessage") }
			.doOnNext { Mono.fromRunnable<Void> { sinks().tryEmitNext("Got the file Information, Processing It...") }.subscribeOn(Schedulers.parallel()).subscribe() }
			.doOnNext { processedMessage -> processFileAndEmit(processedMessage.message) }
			.subscribe()
	}

	private fun processFileAndEmit(fileName: String) {
		val fileReaderDetails = processFile(fileName)

		// Create a map of the details
		val detailsMap = mapOf(
			"lines" to fileReaderDetails.lines,
			"words" to fileReaderDetails.words,
			"letters" to fileReaderDetails.letters
		)

		// Convert the map to a JSON string
		val objectMapper = jacksonObjectMapper()
		val jsonString = objectMapper.writeValueAsString(detailsMap)

		// Emit the JSON string
		sinks().tryEmitNext(jsonString)
	}

	fun processFile(fileName: String): FileReaderDetails {
		var lines = 0
		var words = 0
		var letters = 0

		try {
			Files.lines(Paths.get(fileLookupPath, fileName), StandardCharsets.ISO_8859_1).use { stream ->
				stream.forEach { line ->
					lines++
					words += line.split("\\s+".toRegex()).size
					letters += line.replace("\\s".toRegex(), "").length
				}
			}
		} catch (e: IOException) {
			sinks().tryEmitNext("Error reading file")
			println("Error reading file: ${e.message}")
		}

		println("Read $words words, $letters letters, and $lines lines from file $fileName")
		return FileReaderDetails(lines, words, letters)
	}
}



data class FileReaderDetails(val lines: Int, val words: Int, val letters: Int)

@Component
class MyWebSocketHandler(private val sinks: Sinks.Many<String>) : WebSocketHandler {

	override fun handle(session: WebSocketSession): Mono<Void> {
		return session.send(
			sinks.asFlux().map(session::textMessage)
		).and(
			session.receive().map { webSocketMessage -> sinks.tryEmitNext(webSocketMessage.payloadAsText) }
		).then()
	}
}

fun main(args: Array<String>) {
	runApplication<FileReaderKotlinApplication>(*args)
}
