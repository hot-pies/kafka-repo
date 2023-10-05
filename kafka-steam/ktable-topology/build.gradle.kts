plugins {
    id("java")
    id("io.freefair.lombok") version "8.3"
}

group = "com.coolhand.kafka.steam"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation( "org.junit.jupiter:junit-jupiter-engine:5.10.0")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.10.0")
    testImplementation("org.apache.kafka:kafka-streams-test-utils:3.5.1")
    implementation("org.apache.kafka:kafka-streams:3.5.1")
    implementation("org.slf4j:slf4j-log4j12:1.7.25")
    implementation("org.slf4j:slf4j-api:1.7.25")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.15.2")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.15.2")
}

tasks.test {
    useJUnitPlatform()
}