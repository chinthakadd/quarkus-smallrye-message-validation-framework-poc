# smallrye-kafka-watermarker

This project uses Quarkus, the Supersonic Subatomic Java Framework.

If you want to learn more about Quarkus, please visit its website: <https://quarkus.io/>.

## Running the application in dev mode

You can run your application in dev mode that enables live coding using:

```shell script
./mvnw quarkus:dev
```

> **_NOTE:_**  Quarkus now ships with a Dev UI, which is available in dev mode only at <http://localhost:8080/q/dev/>.

## Packaging and running the application

The application can be packaged using:

```shell script
./mvnw package
```

It produces the `quarkus-run.jar` file in the `target/quarkus-app/` directory.
Be aware that it’s not an _über-jar_ as the dependencies are copied into the `target/quarkus-app/lib/` directory.

The application is now runnable using `java -jar target/quarkus-app/quarkus-run.jar`.

If you want to build an _über-jar_, execute the following command:

```shell script
./mvnw package -Dquarkus.package.jar.type=uber-jar
```

The application, packaged as an _über-jar_, is now runnable using `java -jar target/*-runner.jar`.

## Creating a native executable

You can create a native executable using:

```shell script
./mvnw package -Dnative
```

Or, if you don't have GraalVM installed, you can run the native executable build in a container using:

```shell script
./mvnw package -Dnative -Dquarkus.native.container-build=true
```

SmallRye Kafka Watermarker A Kafka message processing system that implements watermark-based ordering for stream processing. The project provides: Multi-level verification with priority-based verifiers Offset ordering (partition-level) via TopicPartitionOffsetOrderVerifier Late arrival detection via SenderWatermarkOrderVerifier using sender-timestamp headers Extensible verifier architecture with Verifier interface Kafka headers support for sender identification and timestamp tracking Key Features: Ensures messages are processed in order within partitions Detects and rejects late-arriving messages based on sender timestamps Thread-safe watermark management across multiple senders and partitions Integration with Quarkus reactive messaging Use Case: Ideal for stream processing scenarios where message ordering and late arrival detection are critical, such as event sourcing, real-time analytics, or financial trading systems.