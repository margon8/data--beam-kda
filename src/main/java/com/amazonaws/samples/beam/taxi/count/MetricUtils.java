package com.amazonaws.samples.beam.taxi.count;

import com.amazonaws.samples.beam.taxi.count.cloudwatch.Metric;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricUtils {
    private static final Logger LOG = LoggerFactory.getLogger(MetricUtils.class);

    public static class MetricCreator extends DoFn<Long, Metric > {
        @ProcessElement
        public void processElement(@Element Long l, OutputReceiver<Metric> out) {
            Instant ts = Instant.now();
            out.outputWithTimestamp(new Metric(l.longValue(),ts),ts);
        }
    }
}