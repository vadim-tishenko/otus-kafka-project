package ru.cwl.otus.kafka.proj.currentstate;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor

@RestController( )
@RequestMapping("api")
public class ApiWebController {

    private final StreamsBuilderFactoryBean factoryBean;
//    private final KafkaStreams streams;

//    private ReadOnlyKeyValueStore<Long, String> store;

//    @PostConstruct
//    void aaa(){
//        KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
////        ReadOnlyKeyValueStore<Long, String> counts = kafkaStreams.store(
////                StoreQueryParameters.fromNameAndType("store-2", QueryableStoreTypes.keyValueStore())
////        );
////        ReadOnlyKeyValueStore<String, Long> keyValueStore =
////                streams.store("CountsKeyValueStore", QueryableStoreTypes.keyValueStore());
////        ReadOnlyKeyValueStore<Long, String>
//                store = kafkaStreams.
//                store(StoreQueryParameters.fromNameAndType("store-2", QueryableStoreTypes.keyValueStore()));
//        log.info("inited");
//    }


    @GetMapping
    String req1(){
        return "OK";
    }

    @GetMapping("all")
    Map<Long,String> all(){
        KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();

        ReadOnlyKeyValueStore<Long, String> store = kafkaStreams.
                store(StoreQueryParameters.fromNameAndType("store-2", QueryableStoreTypes.keyValueStore()));
        log.info("inited");

        Map<Long, String> result=new HashMap<>();
        store.all().forEachRemaining((k)->{
            final Long key = k.key;
            final String value = k.value;
            result.put(key,value);
        });

        return result;
    }
}
