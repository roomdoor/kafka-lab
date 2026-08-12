plugins {
	kotlin("jvm") version "2.2.21"
	kotlin("plugin.serialization") version "2.2.21"
	application
	// 앱 테스트가 이 모듈을 끌어다 쓰므로 의존성을 api 로 노출해야 한다.
	`java-library`
	// 컨테이너에 넣을 실행 가능한 fat jar 를 만든다.
	id("com.gradleup.shadow") version "9.6.1"
}

group = "com.roomdoor"
version = "0.0.1"

repositories {
	mavenCentral()
}

val ktorVersion = "3.5.2"

dependencies {
	api("io.ktor:ktor-server-core:$ktorVersion")
	api("io.ktor:ktor-server-netty:$ktorVersion")
	api("io.ktor:ktor-server-content-negotiation:$ktorVersion")
	api("io.ktor:ktor-serialization-kotlinx-json:$ktorVersion")
	runtimeOnly("ch.qos.logback:logback-classic:1.5.20")
}

kotlin {
	jvmToolchain(24)
}

application {
	mainClass = "com.roomdoor.mockpg.MockPaymentGatewayKt"
}
