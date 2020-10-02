/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package com.amazonaws.samples.beam.taxi.count.streamborough;

import com.amazonaws.regions.Regions;
import com.amazonaws.samples.beam.taxi.count.*;
import com.amazonaws.samples.beam.taxi.count.cloudwatch.Metric;
import com.amazonaws.samples.beam.taxi.count.kinesis.TripEvent;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kinesis.KinesisIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.ArrayUtils;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;


public class StreamBoroughTaxiCountSolution {

  private static final Logger LOG = LoggerFactory.getLogger(StreamBoroughTaxiCountSolution.class);

  public static void main(String[] args) {
    String[] kinesisArgs = TaxiCountOptions.argsFromKinesisApplicationProperties(args,"BeamApplicationProperties");

    TaxiCountOptions options = PipelineOptionsFactory.fromArgs(ArrayUtils.addAll(args, kinesisArgs)).as(TaxiCountOptions.class);

    options.setRunner(FlinkRunner.class);
    options.setAwsRegion(Regions.getCurrentRegion().getName());

    PipelineOptionsValidator.validate(TaxiCountOptions.class, options);

    Pipeline p = Pipeline.create(options);


    LOG.info("Running pipeline with options: {}", options.toString());

    int batchSize  = 1;
    PCollection<TripEvent> input = p
            .apply("Kinesis source", KinesisIO
                    .read()
                    .withStreamName(options.getInputStreamName())
                    .withAWSClientsProvider(new DefaultCredentialsProviderClientsProvider(Regions.fromName(options.getAwsRegion())))
                    .withInitialPositionInStream(InitialPositionInStream.LATEST)
            )
            .apply("Parse Kinesis events", ParDo.of(new EventParser.KinesisParser()));

    LOG.info("Start consuming events from stream {}", options.getInputStreamName());


    PCollection<TripEvent> window = input
        .apply("Group into 5 second windows", Window
            .<TripEvent>into(FixedWindows.of(Duration.standardSeconds(5)))
            .triggering(AfterWatermark
                .pastEndOfWindow()
                .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(15)))
            )
            .withAllowedLateness(Duration.ZERO)
            .discardingFiredPanes()
        );


    PCollection<Metric> metrics = window
          .apply("Partition by borough", ParDo.of(new PartitionByBorough()))
          .apply("Count per borough", Count.perKey())
          .apply("Map to Metric", ParDo.of(new MetricUtils.MetricKVCreator()));

    String streamName = options.getInputStreamName()==null ? "Unknown" : options.getInputStreamName();
    Dimension dimension = Dimension.builder().name("StreamName").value(streamName).build();

    metrics
        .apply("Void key", WithKeys.of((Void) null))
        .apply("Global Metric window", Window.<KV<Void, Metric>>into(new GlobalWindows())
            .triggering(Repeatedly.forever(AfterFirst.of(
                AfterPane.elementCountAtLeast(20),
                AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(1)))))
            .discardingFiredPanes()
        )
        .apply("Group into batches", GroupIntoBatches.ofSize(batchSize))
        .apply("CloudWatch sink", ParDo.of(new CloudWatchSink(dimension)));


    p.run().waitUntilFinish();
  }

}