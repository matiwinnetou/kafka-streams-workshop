package kafkastreams.javaexercises;

import com.fasterxml.jackson.databind.JsonNode;
import kafkastreams.serdes.JsonNodeSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;

public class Exercise_2_Aggregations {

    private Serde<String> strings = Serdes.String();
    private Serde<Integer> ints = Serdes.Integer();
    private Serde<Long> longs = Serdes.Long();
    private Serde<JsonNode> json = new JsonNodeSerde();

    /**
     * Read the topic 'colors' and count the number of occurrences of
     * each color *key*. Write the result to the topic 'color-counts'.
     */
    public void countColorKeyOccurrences(StreamsBuilder builder) {
        KTable<String, Long> colorCounts = builder.stream("colors", Consumed.with(strings, strings))
                .groupBy((key, value) -> key, Serialized.with(strings, strings))
                .count(Materialized.as("color-counts"));

        colorCounts.toStream().to("color-counts", Produced.with(strings, longs));
    }

    /**
     * Read the topic 'colors' and count the number of occurrences of
     * each color *value*. Write the result to the topic 'color-counts'.
     */
    public void countColorValueOccurrences(StreamsBuilder builder) {
        KTable<String, Long> colorCounts = builder.stream("colors", Consumed.with(strings, strings))
                .groupBy((key, value) -> value, Serialized.with(strings, strings))
                .count(Materialized.as("color-counts"));

        colorCounts.toStream().to("color-counts", Produced.with(strings, longs));
    }

    /**
     * Read the topic 'hamlet' and count the number of occurrences
     * of each word in the text. Write the result to the topic
     * 'word-counts'.
     */
    public void countWordOccurrences(StreamsBuilder builder) {
        KStream<String, String> hamlet = builder.stream("hamlet", Consumed.with(strings, strings));

        KStream<String, String> stringStringKStream = hamlet
                .flatMapValues(words -> () -> Arrays.stream(words.split(" "))
                        .iterator());

        KTable<String, Long> count = stringStringKStream
                .mapValues(val -> val.toLowerCase())
                .groupBy((key, word) -> word, Serialized.with(strings, strings))
                .count();

        count.toStream().to("word-counts", Produced.with(strings, longs));
    }

    /**
     * Read the topic 'click-events' and count the number of events
     * per site (field 'provider.@id'). Write the results to the topic
     * 'clicks-per-site'.
     */
    public void clicksPerSite(StreamsBuilder builder) {
        KStream<String, JsonNode> stream = builder.stream("click-events", Consumed.with(strings, json));

        KTable<String, Long> provider = stream.groupBy((key, value) -> value.get("provider").get("@id").asText(), Serialized.with(strings, json)).count();

        provider.toStream().to("clicks-per-site", Produced.with(strings, longs));
    }

    /**
     * Read the topic 'prices' and compute the total price per site (key).
     * Write the results to the topic 'total-price-per-site'.
     *
     * Hint: Use method 'reduce' on the grouped stream.
     */
    public void totalPricePerSite(StreamsBuilder builder) {
//        KTable<Object, Long> prices = builder.stream("prices")
//                .groupBy((key, value) -> key)
//                .reduce()
    }

    /**
     * Read the topic 'click-events' and compute the total value
     * (field 'object.price') of the classified ads per site. Write
     * the results to the topic 'total-classifieds-price-per-site'.
     */
    public void totalClassifiedsPricePerSite(StreamsBuilder builder) {

    }

    /**
     * Read the topic 'click-events' and count the number of events
     * per site (field 'provider.@id') per hour. Write the results to
     * the state store 'clicks-per-hour'.
     */
    public void clicksPerHour(StreamsBuilder builder) {

    }

}
