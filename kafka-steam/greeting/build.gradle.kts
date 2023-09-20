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

//    testImplementation(platform("org.junit:junit-bom:5.10.0"))
//    testImplementation("junit:junit:4.13.2")
    testImplementation("org.apache.kafka:kafka-streams-test-utils:3.5.1")

    // https://mvnrepository.com/artifact/org.apache.kafka/kafka-streams
    implementation("org.apache.kafka:kafka-streams:3.5.1")
// https://mvnrepository.com/artifact/org.slf4j/slf4j-log4j12
    implementation("org.slf4j:slf4j-log4j12:1.7.25")
// https://mvnrepository.com/artifact/org.slf4j/slf4j-api
    implementation("org.slf4j:slf4j-api:1.7.25")
    // https://mvnrepository.com/artifact/org.projectlombok/lombok


}

tasks.test {
    useJUnitPlatform()
}