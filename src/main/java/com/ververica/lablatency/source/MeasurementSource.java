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
package com.ververica.lablatency.source;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import com.ververica.lablatency.event.Measurement;
import com.ververica.lablatency.util.TimeUtil;

import java.util.List;
import java.util.Random;

public class MeasurementSource extends RichParallelSourceFunction<Measurement> {

    private final Random rand;
    private final int spikeInterval;
    private final int waitMicro;
    private final List<Measurement> measurements;

    private transient volatile boolean cancelled;

    public MeasurementSource(int spikeInterval, int waitMicro) {
        this.rand = new Random();
        this.spikeInterval = spikeInterval;
        this.waitMicro = waitMicro;
        this.measurements = MeasurementGenerator.generateMeasurements();
    }

    @Override
    public void run(SourceContext<Measurement> sourceContext) throws Exception {
        while (!cancelled) {

            // simulate measurement spikes every spikeInterval minute
            if (System.currentTimeMillis() / 1000 / 60 % spikeInterval != 0) {
                Thread.sleep(1);
            }

            TimeUtil.busyWaitMicros(this.waitMicro);

            int index = rand.nextInt(measurements.size());

            synchronized (sourceContext.getCheckpointLock()) {
                sourceContext.collect(measurements.get(index));
            }
        }
    }

    @Override
    public void cancel() {
        this.cancelled = true;
    }
}
