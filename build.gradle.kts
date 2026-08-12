import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
	kotlin("jvm") version "2.2.21"
	kotlin("plugin.spring") version "2.2.21"
	kotlin("plugin.jpa") version "2.2.21"
	id("org.springframework.boot") version "4.0.7"
	id("io.spring.dependency-management") version "1.1.7"
}

group = "com.roomdoor"
version = "0.0.1-SNAPSHOT"

java {
	toolchain {
		languageVersion = JavaLanguageVersion.of(24)
	}
}

repositories {
	mavenCentral()
}

// Spring Boot BOM 은 코루틴을 1.10.2 로 고정하는데, 테스트에서 함께 쓰는 Ktor 3.5 는 1.11.0 이 필요하다.
// 그대로 두면 mock 게이트웨이를 띄우는 순간 NoSuchMethodError 로 죽는다.
extra["kotlin-coroutines.version"] = "1.11.0"

dependencies {
	implementation("org.springframework.boot:spring-boot-starter-webmvc")
	implementation("org.springframework.boot:spring-boot-starter-data-jpa")
	implementation("org.springframework.boot:spring-boot-starter-actuator")
	// spring-kafka 라이브러리만 넣으면 자동 설정이 딸려오지 않는다.
	// Spring Boot 4 부터 자동 설정이 기술별 모듈로 쪼개졌고, KafkaAdmin(토픽 생성)과
	// @KafkaListener 처리기(@EnableKafka)는 이 스타터에 들어 있다.
	implementation("org.springframework.boot:spring-boot-starter-kafka")
	implementation("org.springdoc:springdoc-openapi-starter-webmvc-ui:3.1.0")
	implementation("org.jetbrains.kotlin:kotlin-reflect")
	implementation("tools.jackson.module:jackson-module-kotlin")
	runtimeOnly("org.postgresql:postgresql")

	// mock 결제 게이트웨이는 별도 모듈이다. 테스트에서만 끌어다 임의 포트로 직접 띄운다.
	// (컨테이너로 띄우면 테스트가 이미지 빌드에 묶인다.) 앱 런타임에는 Ktor 가 들어가지 않는다.
	testImplementation(project(":mock-pg"))
	testImplementation("org.springframework.boot:spring-boot-starter-test")
	testImplementation("org.springframework.kafka:spring-kafka-test")
	testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
	// Testcontainers 2.x 부터 모듈 아티팩트 이름이 testcontainers-* 로 바뀌었다 (예전 org.testcontainers:kafka 아님)
	testImplementation("org.testcontainers:testcontainers-junit-jupiter")
	testImplementation("org.testcontainers:testcontainers-kafka")
	testImplementation("org.testcontainers:testcontainers-postgresql")
	testImplementation("org.awaitility:awaitility")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

kotlin {
	compilerOptions {
		jvmTarget = JvmTarget.JVM_24
		freeCompilerArgs.addAll("-Xjsr305=strict", "-Xannotation-default-target=param-property")
	}
}

allOpen {
	annotation("jakarta.persistence.Entity")
	annotation("jakarta.persistence.MappedSuperclass")
	annotation("jakarta.persistence.Embeddable")
}

tasks.withType<Test> {
	useJUnitPlatform()
}
