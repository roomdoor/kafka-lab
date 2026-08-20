package com.roomdoor.kafkalab.config

import org.aspectj.lang.ProceedingJoinPoint
import org.aspectj.lang.annotation.Around
import org.aspectj.lang.annotation.Aspect
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

/**
 * 컨트롤러와 서비스의 메서드 호출을 들어갈 때/나올 때 한 줄씩 남긴다.
 *
 * 이벤트 드리븐은 흐름이 코드에 이어져 있지 않아서 눈으로 따라가기 어렵다.
 * HTTP 스레드에서 시작한 일이 릴레이 스레드로, 다시 컨슈머 스레드로 건너간다.
 * 로그 패턴에 스레드 이름이 들어 있으므로 그 건너뛰는 지점이 보인다.
 *
 * 메서드마다 log.info 를 넣지 않은 이유가 이것이다. 서비스가 늘어도 여기는 그대로다.
 *
 * 잡히는 대상은 `@RestController` 와 `@Service` 클래스의 **public 메서드**다.
 * 컨슈머([com.roomdoor.kafkalab.payment.PaymentConsumer] 등)는 `@Component` 라 안 잡히는데,
 * 이미 자기 흐름을 직접 로그로 남기고 있어서 중복을 피한 것이다.
 * 필요하면 아래 포인트컷에 `@within(org.springframework.stereotype.Component)` 를 더하면 된다.
 *
 * Spring AOP 는 프록시 기반이라 **같은 객체 안에서의 호출은 안 잡힌다.** private 헬퍼도 마찬가지다.
 */
@Aspect
@Component
class CallLoggingAspect {

	private val log = LoggerFactory.getLogger(javaClass)

	/** 중첩 깊이만큼 들여쓴다. 스레드마다 따로 세야 컨슈머 스레드와 HTTP 스레드가 섞이지 않는다. */
	private val depth = ThreadLocal.withInitial { 0 }

	@Around(
		"@within(org.springframework.web.bind.annotation.RestController)" +
			" || @within(org.springframework.stereotype.Service)",
	)
	fun logCall(joinPoint: ProceedingJoinPoint): Any? {
		val target = "${joinPoint.signature.declaringType.simpleName}.${joinPoint.signature.name}"
		val indent = "  ".repeat(depth.get())
		val startedAt = System.nanoTime()

		log.info("$indent→ $target(${joinPoint.args.joinToString(", ") { summarize(it) }})")
		depth.set(depth.get() + 1)

		try {
			val result = joinPoint.proceed()
			log.info("$indent← $target ${elapsedMillis(startedAt)}ms")
			return result
		} catch (e: Throwable) {
			// 예외도 흐름의 일부다. 여기서 삼키지 않고 남기기만 한다.
			log.warn("$indent✗ $target ${elapsedMillis(startedAt)}ms ${e.javaClass.simpleName}: ${e.message}")
			throw e
		} finally {
			depth.set(depth.get() - 1)
		}
	}

	private fun elapsedMillis(startedAt: Long) = (System.nanoTime() - startedAt) / 1_000_000

	/**
	 * 인자는 toString 에 맡기고 길면 자른다. 읽을 만하게 만드는 건 각 타입의 몫이다 —
	 * data class 는 공짜고, JPA 엔티티는 toString 을 직접 달아야 `Payment@1a2b` 를 면한다.
	 */
	private fun summarize(arg: Any?): String = arg?.toString()?.take(120) ?: "null"
}
