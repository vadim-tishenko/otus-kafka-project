package ru.cwl.otus.kafka.proj.currentstate.kafka;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
public class WordCountProcessor {

    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final Serde<Long> LONG_SERDE = Serdes.Long();

    //    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, String> messageStream = streamsBuilder
                .stream("input-topic", Consumed.with(STRING_SERDE, STRING_SERDE));

        KTable<String, Long> wordCounts = messageStream
                .mapValues((ValueMapper<String, String>) String::toLowerCase)
                .flatMapValues(value -> Arrays.asList(value.split("\\W+")))
                .groupBy((key, word) -> word, Grouped.with(STRING_SERDE, STRING_SERDE))
                .count();

        wordCounts.toStream().to("output-topic");
    }

    @Autowired
    void buildPL(StreamsBuilder streamsBuilder) {
        String storeName = "store-2";
        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(storeName);
        StoreBuilder<KeyValueStore<Long, String>> storeBuilder = Stores.keyValueStoreBuilder(storeSupplier, Serdes.Long(), Serdes.String());



        KStream<Long, String> messageStream = streamsBuilder
                .stream("topic-2", Consumed.with(LONG_SERDE, STRING_SERDE));
        streamsBuilder.addStateStore(storeBuilder);
//        0              1     2                 3   4   5   6     7       8        9               10           11
//        gmt_event_time,id_tr,is_valid_location,lat,lon,alt,speed,heading,id_event,gmtlocationtime,gmt_sys_time,unit_recnum
//        1707092159,925007,1,56.735990,61.094960,247,0,261,24,,1707092172,1

        messageStream
//                .filter((k, v) -> k == 925007L)
                .processValues(MyProc::new,storeName)
                .mapValues(v -> {
                    final String[] s = v.split(",");
                    long gmtTime = Long.parseLong(s[0]);
                    long gmtSysTime = Long.parseLong(s[10]);
                    long diff = gmtSysTime - gmtTime;
                    return s[0] + ";" + s[10] + ";" + diff;
                })
                .print(Printed.toSysOut());


    }
}