package com.ververica.lablatency.source;

import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import com.ververica.lablatency.event.Measurement;
import com.ververica.lablatency.util.TimeUtil;

import java.util.List;
import java.util.Random;

public class MeasurementSource extends RichParallelSourceFunction<Measurement>
        implements CheckpointedFunction {

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
    public void snapshotState(FunctionSnapshotContext context) throws Exception {}

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {}

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
