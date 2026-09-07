package com.iotpipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iotpipeline.model.SensorEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.util.Collector;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PoolingOptions;

import java.io.InputStream;
import java.time.Instant;
import java.util.Date;

public class FlinkConsumerJob {

    public static void main(String[] args) throws Exception {
        InputStream propertiesStream = FlinkConsumerJob.class.getResourceAsStream("/application.properties");
        if (propertiesStream == null) throw new RuntimeException("Could not find application.properties");
        ParameterTool params = ParameterTool.fromPropertiesFile(propertiesStream).mergeWith(ParameterTool.fromArgs(args));

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        // Enable Checkpointing every 5 seconds (5000 ms).
        // This makes Flink save its offsets internally for fault tolerance, 
        // AND automatically commits them to Kafka so they show up in Kafka UI!
        env.enableCheckpointing(5000);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(params.getRequired("kafka.bootstrap.servers"))
                .setTopics(params.getRequired("kafka.topic"))
                .setGroupId(params.getRequired("kafka.group.id")) 
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        DataStream<SensorEvent> parsedStream = kafkaStream
                .map(new MapFunction<String, SensorEvent>() {
                    private transient ObjectMapper jsonParser;
                    @Override
                    public SensorEvent map(String value) throws Exception {
                        if (jsonParser == null) { jsonParser = new ObjectMapper(); }
                        return jsonParser.readValue(value, SensorEvent.class);
                    }
                });

        // SINK 1: ALL EVENTS
        DataStream<Tuple8<String, Date, String, String, String, Integer, String, String>> allEventsStream = parsedStream
                .map(new MapFunction<SensorEvent, Tuple8<String, Date, String, String, String, Integer, String, String>>() {
                    private transient ObjectMapper mapper;
                    @Override
                    public Tuple8<String, Date, String, String, String, Integer, String, String> map(SensorEvent event) throws Exception {
                        if (mapper == null) mapper = new ObjectMapper();
                        return new Tuple8<>(event.getDevice_id(), Date.from(Instant.parse(event.getTimestamp())), event.getDevice_type(),
                                event.getRegion(), event.getSite(), event.getBattery_percent(), event.getStatus(), mapper.writeValueAsString(event.getReadings()));
                    }
                });
        CassandraSink.addSink(allEventsStream)
                .setQuery("INSERT INTO iot_pipeline.sensor_readings_by_device (device_id, timestamp, device_type, region, site, battery_percent, status, readings) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
                .setMaxConcurrentRequests(50) // Throttle Flink so it doesn't overwhelm Docker Cassandra
                .setClusterBuilder(getClusterBuilder(params)).build();

        // SINK 2: ALERTS
        DataStream<Tuple7<String, String, Date, String, String, Integer, String>> alertsStream = parsedStream
                .filter(event -> !"OK".equals(event.getStatus()))
                .map(new MapFunction<SensorEvent, Tuple7<String, String, Date, String, String, Integer, String>>() {
                    private transient ObjectMapper mapper;
                    @Override
                    public Tuple7<String, String, Date, String, String, Integer, String> map(SensorEvent event) throws Exception {
                        if (mapper == null) mapper = new ObjectMapper();
                        return new Tuple7<>(event.getSite(), event.getStatus(), Date.from(Instant.parse(event.getTimestamp())),
                                event.getDevice_id(), event.getDevice_type(), event.getBattery_percent(), mapper.writeValueAsString(event.getReadings()));
                    }
                });
        CassandraSink.addSink(alertsStream)
                .setQuery("INSERT INTO iot_pipeline.alerts_by_site (site, status, timestamp, device_id, device_type, battery_percent, readings) VALUES (?, ?, ?, ?, ?, ?, ?)")
                .setMaxConcurrentRequests(50)
                .setClusterBuilder(getClusterBuilder(params)).build();

        // SINK 3: GPS
        DataStream<Tuple7<String, Date, String, Double, Double, Double, Integer>> gpsStream = parsedStream
                .filter(event -> "gps".equals(event.getDevice_type()))
                .map(new MapFunction<SensorEvent, Tuple7<String, Date, String, Double, Double, Double, Integer>>() {
                    @Override
                    public Tuple7<String, Date, String, Double, Double, Double, Integer> map(SensorEvent event) {
                        return new Tuple7<>(event.getSite(), Date.from(Instant.parse(event.getTimestamp())), event.getDevice_id(),
                                ((Number) event.getReadings().get("latitude")).doubleValue(), ((Number) event.getReadings().get("longitude")).doubleValue(),
                                ((Number) event.getReadings().get("speed_kmh")).doubleValue(), ((Number) event.getReadings().get("heading_degrees")).intValue());
                    }
                });
        CassandraSink.addSink(gpsStream)
                .setQuery("INSERT INTO iot_pipeline.gps_tracking_by_vehicle (site, timestamp, device_id, latitude, longitude, speed_kmh, heading_degrees) VALUES (?, ?, ?, ?, ?, ?, ?)")
                .setMaxConcurrentRequests(50)
                .setClusterBuilder(getClusterBuilder(params)).build();

        // SINK 4: INDUSTRIAL
        DataStream<Tuple5<String, String, Date, String, String>> industrialStream = parsedStream
                .filter(event -> "industrial_multi".equals(event.getDevice_type()))
                .map(new MapFunction<SensorEvent, Tuple5<String, String, Date, String, String>>() {
                    private transient ObjectMapper mapper;
                    @Override
                    public Tuple5<String, String, Date, String, String> map(SensorEvent event) throws Exception {
                        if (mapper == null) mapper = new ObjectMapper();
                        return new Tuple5<>(event.getSite(), event.getDevice_type(), Date.from(Instant.parse(event.getTimestamp())),
                                event.getDevice_id(), mapper.writeValueAsString(event.getReadings()));
                    }
                });
        CassandraSink.addSink(industrialStream)
                .setQuery("INSERT INTO iot_pipeline.industrial_events_by_site (site, device_type, timestamp, device_id, readings) VALUES (?, ?, ?, ?, ?)")
                .setMaxConcurrentRequests(50)
                .setClusterBuilder(getClusterBuilder(params)).build();

        // SINK 5: AGGREGATIONS
        DataStream<Tuple4<String, Date, Double, Integer>> avgTempStream = parsedStream
                .filter(event -> "temperature_humidity".equals(event.getDevice_type()))
                .keyBy(SensorEvent::getSite)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<SensorEvent, Tuple4<String, Date, Double, Integer>, String, TimeWindow>() {
                    @Override
                    public void process(String site, Context context, Iterable<SensorEvent> elements, Collector<Tuple4<String, Date, Double, Integer>> out) {
                        double sumTemp = 0; int count = 0;
                        for (SensorEvent event : elements) {
                            if (event.getReadings() != null && event.getReadings().containsKey("temperature_celsius")) {
                                sumTemp += ((Number) event.getReadings().get("temperature_celsius")).doubleValue();
                                count++;
                            }
                        }
                        if (count > 0) out.collect(new Tuple4<>(site, new Date(context.window().getEnd()), sumTemp / count, count));
                    }
                });
        CassandraSink.addSink(avgTempStream)
                .setQuery("INSERT INTO iot_pipeline.average_temperature_by_site (site, window_end, avg_temp_celsius, events_processed) VALUES (?, ?, ?, ?)")
                .setMaxConcurrentRequests(50)
                .setClusterBuilder(getClusterBuilder(params)).build();

        avgTempStream.print("AVG TEMP -> ");
        env.execute("IoT Sensor Flink Consumer");
    }

    private static ClusterBuilder getClusterBuilder(ParameterTool params) {
        return new ClusterBuilder() {
            @Override
            protected Cluster buildCluster(Cluster.Builder builder) {
                // Increase the queue size to handle the massive async load from 5 parallel sinks
                PoolingOptions poolingOptions = new PoolingOptions();
                poolingOptions.setMaxQueueSize(8192); 

                return builder.addContactPoint(params.getRequired("cassandra.host"))
                              .withPort(params.getInt("cassandra.port", 9042))
                              .withPoolingOptions(poolingOptions)
                              .build();
            }
        };
    }
}
