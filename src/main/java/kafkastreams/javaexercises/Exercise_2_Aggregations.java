package kafkastreams.javaexercises;

import com.fasterxml.jackson.databind.JsonNode;
import kafkastreams.serdes.JsonNodeSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.TimeWindow;

import java.util.Arrays;

public class Exercise_2_Aggregations {

    private Serde<String> strings = Serdes.String();
    private Serde<Integer> ints = Serdes.Integer();
    private Serde<Long> longs = Serdes.Long();
    private Serde<JsonNode> json = new JsonNodeSerde();
//    private Serde<KeyValue> keyValue = Serdes.serdeFrom(KeyValue);

    /**
     * Read the topic 'colors' and count the number of occurrences of
     * each color *key*. Write the result to the topic 'color-counts'.
     */
    public void countColorKeyOccurrences(StreamsBuilder builder) {
        KTable<String, Long> colorCounts = builder.stream("colors", Consumed.with(strings, strings))
                .groupBy((key, value) -> key, Serialized.with(strings, strings))
                .count();

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
        KTable<String, Integer> pricesPerSite = builder.stream("prices", Consumed.with(strings, ints))
                .groupBy((key, value) -> key, Serialized.with(strings, ints))
                // reducer reduces value, not key
                .reduce((v1, v2) -> v1 + v2, Materialized.with(strings, ints));

        pricesPerSite.toStream().to("total-price-per-site", Produced.with(strings, ints));
    }

    /**
     * Read the topic 'click-events' and compute the total value
     * (field 'object.price') of the classified ads per site. Write
     * the results to the topic 'total-classifieds-price-per-site'.
     */
//    public void totalClassifiedsPricePerSite(StreamsBuilder builder) {
//        KStream<String, Integer> stringIntegerKStream = builder.stream("click-events", Consumed.with(strings, json))
//                .selectKey((key, value) -> value.get("provider").get("@id").asText())
//                .mapValues(value -> value.get("object").has("price") ? value.get("object").get("price").asInt(0) : 0);
//
//        KGroupedStream<String, Integer> stringIntegerKGroupedStream = stringIntegerKStream.groupBy((key, value) -> key, Serialized.with(strings, ints));
//        KTable<String, Integer> reduce = stringIntegerKGroupedStream.reduce((v1, v2) -> v1 + v2, Materialized.with(strings, ints));
//
//        reduce.toStream().to("total-classifieds-price-per-site", Produced.with(strings, ints));
//    }

    // TODO fails now
//    public void totalClassifiedsPricePerSite(StreamsBuilder builder) {
//        KStream<String, JsonNode> stringIntegerKStream = builder.stream("click-events", Consumed.with(strings, json));
//        KGroupedStream<String, KeyValue> stringIntegerKGroupedStream = stringIntegerKStream.groupBy((key, value) -> value.get("provider").get("@id").asText(), Serialized.with(strings, json));
//
//        KTable<String, Integer> reduce = stringIntegerKGroupedStream.reduce((v1, v2) -> v1 + v2, Materialized.with(strings, ints));
//
//        reduce.toStream().to("total-classifieds-price-per-site", Produced.with(strings, ints));
//    }

    public void totalClassifiedsPricePerSite(StreamsBuilder builder) {
    }

    /**
     * Read the topic 'click-events' and count the number of events
     * per site (field 'provider.@id') per hour. Write the results to
     * the state store 'clicks-per-hour'.
     */
    public void clicksPerHour(StreamsBuilder builder) {
        KStream<String, JsonNode> clickEvents = builder.stream("click-events", Consumed.with(strings, json));
        KGroupedStream<String, JsonNode> groupedBySite = clickEvents.groupBy((key, value) -> value.get("provider").get("@id").asText(), Serialized.with(strings, json));
        TimeWindowedKStream<String, JsonNode> stringJsonNodeTimeWindowedKStream = groupedBySite.windowedBy(TimeWindows.of(3600 * 1000));

        KTable<Windowed<String>, Long> count = stringJsonNodeTimeWindowedKStream.count();
        count.toStream().to("clicks-per-hour", Produced.with(strings, longs));
    }

}
