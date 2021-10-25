package com.ververica.lablatency.event;

import java.util.Objects;

@SuppressWarnings("unused")
public class EnrichedMeasurement extends Measurement {

    private String locationInfo;

    public EnrichedMeasurement() {}

    public EnrichedMeasurement(
            int sensorId,
            double value,
            String location,
            String measurementInformation,
            String locationInfo) {
        super(sensorId, value, location, measurementInformation);
        this.locationInfo = locationInfo;
    }

    public EnrichedMeasurement(Measurement measurement, String locationInfo) {
        super(measurement);
        this.locationInfo = locationInfo;
    }

    public String getLocationInfo() {
        return locationInfo;
    }

    public void setLocationInfo(String locationInfo) {
        this.locationInfo = locationInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        EnrichedMeasurement that = (EnrichedMeasurement) o;
        return Objects.equals(locationInfo, that.locationInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), locationInfo);
    }

    @Override
    public String toString() {
        return "EnrichedMeasurement{"
                + super.toString()
                + ", locationInfo='"
                + locationInfo
                + '\''
                + "}";
    }
}
