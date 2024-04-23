package com.evolutionnext;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

public class MyProducer {
    @SuppressWarnings("Duplicates")
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();

        String bootstrapServer = Optional.ofNullable(System.getenv(
            "BOOTSTRAP_SERVERS")).orElse("localhost:9092");

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            bootstrapServer);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            IntegerSerializer.class);
        properties.put(ProducerConfig.RETRIES_CONFIG, 30);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 2000);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 2000);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 12000);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 30000);
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 15000);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 20000);

        try (KafkaProducer<String, Integer> producer = new KafkaProducer<>(properties)) {
            String stateString =
                "AK,AL,AZ,AR,CA,CO,CT,DE,FL,GA," +
                "HI,ID,IL,IN,IA,KS,KY,LA,ME,MD," +
                "MA,MI,MN,MS,MO,MT,NE,NV,NH,NJ," +
                "NM,NY,NC,ND,OH,OK,OR,PA,RI,SC," +
                "SD,TN,TX,UT,VT,VA,WA,WV,WI,WY";

            AtomicBoolean done = new AtomicBoolean(false);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                done.set(true);
                producer.flush();
                producer.close();
            }));

            Random random = new Random();

            while (!done.get()) {
                String[] states = stateString.split(",");
                String state = states[random.nextInt(states.length)];
                int amount = random.nextInt(100000 - 50 + 1) + 50;

                ProducerRecord<String, Integer> producerRecord =
                    new ProducerRecord<String, Integer>("my-orders", state, amount);


                //Asynchronous
                producer.send(producerRecord, (metadata, exception) -> {
                    if (exception != null) {
                        System.out.println("ERROR! ");
                        String firstException =
                            Arrays.stream(exception.getStackTrace())
                                .findFirst()
                                .map(StackTraceElement::toString)
                                .orElse("Undefined Exception");
                        System.out.println(firstException);
                    } else {

                        System.out.println(producerRecord.key());
                        System.out.println(producerRecord.value());

                        if (metadata.hasOffset()) {
                            System.out.format("offset: %d\n",
                                metadata.offset());
                        }
                        System.out.format("partition: %d\n",
                            metadata.partition());
                        System.out.format("timestamp: %d\n",
                            metadata.timestamp());
                        System.out.format("topic: %s\n", metadata.topic());
                        System.out.format("toString: %s\n",
                            metadata.toString());
                    }
                });

                Thread.sleep(random.nextInt(30000 - 1000 + 1) + 1000);
            }
        }
    }
}
