package com.iotpipeline.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Map;

/**
 * A basic POJO to represent the incoming JSON from Kafka.
 * The @JsonIgnoreProperties ensures that if the producer adds a new field 
 * we don't expect, the consumer won't crash.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SensorEvent {

    private int schema_version;
    private String event_id;
    private String device_id;
    private String device_type;
    private String region;
    private String site;
    private String timestamp;
    private Integer battery_percent;
    private String status;
    private Map<String, Object> readings; // Holds dynamic JSON object

    // Empty constructor required for Jackson
    public SensorEvent() {}

    // Getters and Setters
    public int getSchema_version() { return schema_version; }
    public void setSchema_version(int schema_version) { this.schema_version = schema_version; }

    public String getEvent_id() { return event_id; }
    public void setEvent_id(String event_id) { this.event_id = event_id; }

    public String getDevice_id() { return device_id; }
    public void setDevice_id(String device_id) { this.device_id = device_id; }

    public String getDevice_type() { return device_type; }
    public void setDevice_type(String device_type) { this.device_type = device_type; }

    public String getRegion() { return region; }
    public void setRegion(String region) { this.region = region; }

    public String getSite() { return site; }
    public void setSite(String site) { this.site = site; }

    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

    public Integer getBattery_percent() { return battery_percent; }
    public void setBattery_percent(Integer battery_percent) { this.battery_percent = battery_percent; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public Map<String, Object> getReadings() { return readings; }
    public void setReadings(Map<String, Object> readings) { this.readings = readings; }

    @Override
    public String toString() {
        return "SensorEvent{" +
                "device_id='" + device_id + '\'' +
                ", device_type='" + device_type + '\'' +
                ", status='" + status + '\'' +
                '}';
    }
}
