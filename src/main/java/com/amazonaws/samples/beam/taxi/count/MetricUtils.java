package com.amazonaws.samples.beam.taxi.count;

import com.amazonaws.samples.beam.taxi.count.cloudwatch.Metric;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricUtils {
    private static final Logger LOG = LoggerFactory.getLogger(MetricUtils.class);

    public static class MetricCreator extends DoFn<Long, Metric > {

        @ProcessElement
        public void processElement(ProcessContext c) {
            long l = c.element().longValue();
             Instant ts = c.timestamp();

             LOG.debug("adding metric for count");

            c.output(new Metric(l,ts));
        }

    }

    public static class MetricKVCreator extends DoFn<KV<String,Long>, Metric > {

        @ProcessElement
        public void processElement(ProcessContext c) {
            long count = c.element().getValue();
            String borough = c.element().getKey();

            LOG.debug("adding metric for borough {}", borough);

            //Instant ts = Instant.now();
            Instant ts = c.timestamp();

            c.output(new Metric(count, borough, ts));
        }

    }
}