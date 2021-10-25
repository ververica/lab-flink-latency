/*
 * Copyright 2021 Ververica GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ververica.lablatency.event;

import java.util.Arrays;
import java.util.Objects;

@SuppressWarnings({"unused", "RedundantSuppression"})
public class MeasurementRecord {

    private long timestamp;
    private byte[] key;
    private byte[] value;
    private int partition;

    public MeasurementRecord() {}

    public MeasurementRecord(
            final long timestamp, final byte[] key, final byte[] value, final int partition) {
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
        this.partition = partition;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(final long timestamp) {
        this.timestamp = timestamp;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(final byte[] key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(final byte[] value) {
        this.value = value;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(final int partition) {
        this.partition = partition;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final MeasurementRecord that = (MeasurementRecord) o;
        return timestamp == that.timestamp
                && partition == that.partition
                && Arrays.equals(key, that.key)
                && Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(timestamp, partition);
        result = 31 * result + Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }

    @Override
    public String toString() {
        return "MeasurementRecord{"
                + "timestamp="
                + timestamp
                + ", key="
                + Arrays.toString(key)
                + ", value="
                + Arrays.toString(value)
                + ", partition="
                + partition
                + '}';
    }
}
