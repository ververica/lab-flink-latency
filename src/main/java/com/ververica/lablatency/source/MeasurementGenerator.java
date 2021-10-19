package com.ververica.lablatency.source;

import com.ververica.lablatency.event.Measurement;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MeasurementGenerator {
    public static final Logger LOG = LoggerFactory.getLogger(MeasurementGenerator.class);

    public static final int NUM_OF_MEASUREMENTS = 100_000;
    public static final int NUM_OF_SENSOR_ID = 100;
    public static final int LEN_OF_INFO = 64;
    public static final int RANDOM_SEED = 1;

    public static List<Measurement> generateMeasurements() {
        Random rand = new Random(RANDOM_SEED);
        final List<String> locations = readLocationsFromFile();
        List<Measurement> measurements = new ArrayList<>();
        for (int i = 0; i < NUM_OF_MEASUREMENTS; i++) {
            Measurement aMeasurement =
                    new Measurement(
                            rand.nextInt(NUM_OF_SENSOR_ID),
                            rand.nextDouble() * 100,
                            locations.get(rand.nextInt(locations.size())),
                            "More info: " + RandomStringUtils.randomAlphabetic(LEN_OF_INFO));
            measurements.add(aMeasurement);
        }
        return measurements;
    }

    private static List<String> readLocationsFromFile() {
        List<String> locations = new ArrayList<>();
        try (InputStream is = MeasurementGenerator.class.getResourceAsStream("/cities.csv");
                BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
            String city;
            while ((city = br.readLine()) != null) {
                locations.add(city);
            }
        } catch (IOException e) {
            LOG.error("Unable to read cities from file.", e);
            throw new RuntimeException(e);
        }
        return locations;
    }
}
