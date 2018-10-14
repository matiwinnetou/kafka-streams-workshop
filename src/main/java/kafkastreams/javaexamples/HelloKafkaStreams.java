package kafkastreams.javaexamples;

import com.fasterxml.jackson.databind.JsonNode;
import kafkastreams.serdes.JsonNodeSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

public class HelloKafkaStreams extends KafkaStreamsApp {

    public static void main(String[] args) {
        new HelloKafkaStreams().start("hello-world-app");
    }

    public Topology createTopology(StreamsBuilder builder) {
        Serde<String> strings = Serdes.String();
        Serde<JsonNode> json = new JsonNodeSerde();

        KStream<String, JsonNode> namesStream = builder.stream("test", Consumed.with(strings, json));

//        namesStream.mapValues(jsonNode -> "Hello, " + jsonNode.get("name") + "!")
//                .to("hello", Produced.with(strings, strings));

        namesStream.print(Printed.toSysOut());

        return builder.build();
    }

}
