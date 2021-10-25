package com.ververica.lablatency.event;

import java.io.Serializable;
import java.util.Objects;

@SuppressWarnings({"unused", "RedundantSuppression"})
public class Measurement implements Serializable {

    private int sensorId;
    private double value;
    private String location;
    private String measurementInformation;

    public Measurement() {}

    public Measurement(
            final int sensorId,
            final double value,
            final String location,
            final String measurementInformation) {
        this.sensorId = sensorId;
        this.value = value;
        this.location = location;
        this.measurementInformation = measurementInformation;
    }

    public Measurement(Measurement measurement) {
        this.sensorId = measurement.getSensorId();
        this.value = measurement.getValue();
        this.location = measurement.getLocation();
        this.measurementInformation = measurement.getMeasurementInformation();
    }

    public String getMeasurementInformation() {
        return measurementInformation;
    }

    public void setMeasurementInformation(final String measurementInformation) {
        this.measurementInformation = measurementInformation;
    }

    public int getSensorId() {
        return sensorId;
    }

    public void setSensorId(final int sensorId) {
        this.sensorId = sensorId;
    }

    public double getValue() {
        return value;
    }

    public void setValue(final double value) {
        this.value = value;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(final String location) {
        this.location = location;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Measurement that = (Measurement) o;
        return sensorId == that.sensorId
                && Double.compare(that.value, value) == 0
                && Objects.equals(location, that.location)
                && Objects.equals(measurementInformation, that.measurementInformation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sensorId, value, location, measurementInformation);
    }

    @Override
    public String toString() {
        return "Measurement{"
                + "sensorId="
                + sensorId
                + ", value="
                + value
                + ", location='"
                + location
                + '\''
                + ", measurementInformation='"
                + measurementInformation
                + '\''
                + '}';
    }
}