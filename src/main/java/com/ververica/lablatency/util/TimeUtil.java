package com.ververica.lablatency.util;

public class TimeUtil {
    public static void busyWaitMicros(long micros) {
        long waitUntil = System.nanoTime() + (micros * 1_000);
        while (waitUntil > System.nanoTime()) {;
        }
    }
}
