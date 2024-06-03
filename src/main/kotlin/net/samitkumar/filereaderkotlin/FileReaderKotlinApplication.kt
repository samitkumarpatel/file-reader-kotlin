package net.samitkumar.filereaderkotlin

import lombok.AllArgsConstructor
import lombok.extern.slf4j.Slf4j
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
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
import java.io.BufferedReader
import java.io.FileReader

@SpringBootApplication
@Slf4j
@AllArgsConstructor
class FileReaderKotlinApplication {

	@Bean
	fun sinks(): Sinks.Many<String> = Sinks.many().multicast().onBackpressureBuffer()

	@Bean
	fun routerFunction(): RouterFunction<ServerResponse> {
		return RouterFunctions.route()
			.GET("/ping"){
				ServerResponse.ok().bodyValue(mapOf("message" to "PONG"))
			}
			.GET("/details"){ request ->
				val filename = request.queryParams()["filename"]
				ServerResponse.ok().bodyValue(mapOf("filename" to filename))
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


	@Bean
	fun onApplicationEvent(): (ApplicationReadyEvent, ReactiveRedisTemplate<String, String>) -> Unit {
		println("### onApplicationEvent")
		return { event, redisTemplate ->
			redisTemplate.listenToChannel("channel")
				.doOnNext { processedMessage -> println("[*] Received Message: $processedMessage") }
				.doOnNext { sinks().tryEmitNext("Got the file Information, Processing It...") }
				.doOnNext { processedMessage -> processFile(processedMessage.message) }
				.subscribeOn(Schedulers.parallel())
				.subscribe()
		}
	}

	private fun processFile(filePath: String) {
		var lines = 0
		var words = 0
		var letters = 0

		try {
			BufferedReader(FileReader(filePath)).use { reader ->
				var line: String?
				while (reader.readLine().also { line = it } != null) {
					lines++
					words += line!!.split("\\s+").toTypedArray().size
					letters += line!!.replace("\\s".toRegex(), "").length
				}
			}
		} catch (e: Exception) {
			println("Error reading file: ${e.message}")
		}
		println("Read $words words, $letters letters, and $lines lines from file $filePath")

		val x = FileReaderDetails(lines, words, letters)
		val result = mapOf(
			"lines" to x.lines,
			"words" to x.words,
			"letters" to x.letters
		).toString()
		sinks().tryEmitNext(result)
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
