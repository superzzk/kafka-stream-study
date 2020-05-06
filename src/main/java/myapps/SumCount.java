/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package myapps;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.*;
import java.util.concurrent.CountDownLatch;

public class SumCount {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-sum-count");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        builder.<String, String>stream("streams-sum-input")
                .flatMap((k,v) -> {
                    List<KeyValue<String, Integer>> result = new ArrayList<>();
                    Map<String, Integer> value = (Map)JSON.parseObject(v);
                    System.out.println(v);
                    for(Map.Entry<String,Integer> entry : value.entrySet()){
                        if(entry.getKey().equals("time")  ||  entry.getKey().equals("account"))
                            continue;
                        System.out.println("key:" + entry.getKey() + " value:" + entry.getValue());
                        result.add(KeyValue.pair(entry.getKey(),entry.getValue()));
                    }
                    return result;
                })
//                .groupBy((k, v) -> k)
//                .count()
//                .toStream()
//                .groupByKey()
//                .aggregate(
//                        new Initializer<Long>() { /* initializer */
//                            @Override
//                            public Long apply() {
//                                return 0L;
//                            }
//                        },
//                        new Aggregator<String, Long, Long>() { /* adder */
//                            @Override
//                            public Long apply(String key, Long newValue, Long aggValue) {
//                                return aggValue + newValue;
//                            }
//                        },
//                    Materialized.as("aggregated-stream-store").withValueSerde(Serdes.Long())
//                )
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
//                .groupByKey()
                .reduce(Integer::sum).toStream()
//                .count().toStream()
               .to("streams-sum-count-output2", Produced.with(Serdes.String(), Serdes.Integer()));

        final Topology topology = builder.build();
        System.out.println(topology.describe());
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
