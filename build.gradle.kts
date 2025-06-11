plugins {
    kotlin("jvm") version "2.1.21"
    kotlin("plugin.serialization") version "2.1.21"
    application
}

group = "org.hazelcast"
version = "1.0"

val javaVersion = 21
val kotlinVersion = "2.1.21" // can't substitute in plugins section above
val hazelcastVersion = "5.5.0"

repositories {
    mavenCentral()
    maven {
        url = uri("https://packages.confluent.io/maven/")
    }
    maven {
        url = uri("https://repository.hazelcast.com/release/")
    }
    mavenLocal()
    gradlePluginPortal()
}

dependencies {
    testImplementation(kotlin("test"))

    /*
     * Core kotlin libs. We'll need these on all Hazelcast members when using Jet.
     */
    implementation("org.jetbrains.kotlin:kotlin-stdlib:$kotlinVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.8.0")

    /*
     * Hazelcast.
     */
    implementation("com.hazelcast:hazelcast:$hazelcastVersion")
    implementation("com.hazelcast.jet:hazelcast-jet-kafka:$hazelcastVersion")

    /*
     * Kafka
     */
    implementation("com.google.cloud.hosted.kafka:managed-kafka-auth-login-handler:1.0.6")
    implementation("org.slf4j:slf4j-nop:2.0.16") // Quiets an annoying message SLF4J: Failed to load class org.slf4j.impl.StaticLoggerBinder
}

application {
    mainClass.set("org.hazelcast.jetpayments.MainKt")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(javaVersion)
        vendor = JvmVendorSpec.ORACLE
    }
}

kotlin {
    jvmToolchain {
        languageVersion = JavaLanguageVersion.of(javaVersion)
        vendor = JvmVendorSpec.ORACLE
    }
}