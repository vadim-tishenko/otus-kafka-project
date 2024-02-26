package ru.cwl.otus.kafka.proj.currentstate.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Objects;

@Slf4j
public class MyProc implements FixedKeyProcessor<Long, String, String> {
    private KeyValueStore<Long, String> store;
    FixedKeyProcessorContext<Long, String> context;

    @Override
    public void init(FixedKeyProcessorContext<Long, String> context) {
        this.context=context;
        store = context.getStateStore("store-2");
    }

    @Override
    public void process(FixedKeyRecord<Long, String> fixedKeyRecord) {
        final Long key = fixedKeyRecord.key();
        final String v = fixedKeyRecord.value();
        final String state = store.get(key);
        if (state != null) {
            final String[] sps = state.split(",");
            if(sps.length!=3){

            }
            final String time = sps[0];
            final long l0 = Long.parseLong(time);
            final String lat = sps[1];
            final String lon = sps[2];

            final String[] spv = v.split(",");
            final String vTime = spv[0];
            final long l1 = Long.parseLong(vTime);
            final String vLat = spv[3];
            final String vLon = spv[4];

            if(l1>l0){
                String newState=vTime+","+vLat+","+vLon;
                store.put(key,newState);
                log.info("upd state k:{} vo:{} vn:{}",key,state,newState);
            }

        } else {
            final String[] spv = v.split(",");
            final String vTime = spv[0];
            final String vLat = spv[3];
            final String vLon = spv[4];

            String newState=vTime+","+vLat+","+vLon;
            store.put(key,newState);
            log.info("init state k:{} vn:{}",key,newState);
        }


    }

//    @Override
//    public void close() {
//        FixedKeyProcessor.super.close();
//    }
}
