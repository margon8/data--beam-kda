package com.amazonaws.samples.beam.taxi.count

import com.amazonaws.regions.Regions
import com.amazonaws.samples.beam.taxi.count.cloudwatch.Metric
import com.amazonaws.samples.beam.taxi.count.kinesis.TripEvent
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import org.apache.beam.runners.flink.FlinkRunner
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.kinesis.KinesisIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.transforms.windowing.*
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.joda.time.Duration
import org.joda.time.Instant
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.cloudwatch.model.Dimension

object TaxiCountKotlin {

    private val LOG = LoggerFactory.getLogger(TaxiCount::class.java)

    @JvmStatic
    fun main(args: Array<String>) {
        val kinesisArgs = TaxiCountOptions.argsFromKinesisApplicationProperties(args, "BeamApplicationProperties")


        val options = PipelineOptionsFactory.fromArgs(*kinesisArgs).create().`as`(TaxiCountOptions::class.java)
        options.runner = FlinkRunner::class.java
        options.awsRegion = Regions.getCurrentRegion().name

        val pipeline = Pipeline.create(options)

        val inputsDir = "data/*"
        val outputsPrefix = "outputs-kotlin/part"
        val batchSize = 1

        val input = pipeline
                .apply("Kinesis source", KinesisIO
                        .read()
                        .withStreamName(options.getInputStreamName())
                        .withAWSClientsProvider(DefaultCredentialsProviderClientsProvider(Regions.fromName(options.getAwsRegion())))
                        .withInitialPositionInStream(InitialPositionInStream.LATEST)
                )
                .apply("Parse Kinesis events", ParDo.of(EventParser.KinesisParser()))

        val window: PCollection<TripEvent> = input
                .apply("Group into 5 second windows", Window
                        .into<TripEvent>(FixedWindows.of(Duration.standardSeconds(5)))
                        .triggering(AfterWatermark
                                .pastEndOfWindow()
                                .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(15)))
                        )
                        .withAllowedLateness(Duration.ZERO)
                        .discardingFiredPanes()
                )

        val metrics = window
                .apply("Count globally", Count.globally())
                .apply("Map to Metric", ParDo.of(MetricUtils.MetricCreator()))


        val streamName = if (options.inputStreamName == null) "Unknown" else options.inputStreamName
        val dimension = Dimension.builder().name("StreamName").value(streamName).build()

        val output = metrics
                .apply("Void key", WithKeys.of<Void, Metric>(null as Void?))
                .apply("Global Metric window", Window.into<KV<Void, Metric>>(GlobalWindows())
                        .triggering(Repeatedly.forever(AfterFirst.of(
                                AfterPane.elementCountAtLeast(20),
                                AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(1)))))
                        .discardingFiredPanes()
                )
                .apply("Group into batches", GroupIntoBatches.ofSize<Void, Metric>(batchSize.toLong()))
                .apply("CloudWatch sink", ParDo.of(CloudWatchSink(dimension)))

        pipeline.run().waitUntilFinish()
    }



}
