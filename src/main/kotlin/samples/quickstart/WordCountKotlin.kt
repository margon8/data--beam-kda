package samples.quickstart

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.Count
import org.apache.beam.sdk.transforms.Filter
import org.apache.beam.sdk.transforms.FlatMapElements
import org.apache.beam.sdk.transforms.MapElements
import org.apache.beam.sdk.transforms.ProcessFunction
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.TypeDescriptors

object WordCountKotlin {

    @JvmStatic
    fun main(args: Array<String>) {

        val inputsDir = "data/*"
        val outputsPrefix = "outputs-kotlin/part"
        val options = PipelineOptionsFactory.fromArgs(*args).create()

        val pipeline = Pipeline.create(options)

        pipeline
                .apply<PCollection<String>>("Read lines", TextIO.read().from(inputsDir))
                .apply("Find words", FlatMapElements.into(TypeDescriptors.strings())
                        .via(ProcessFunction<String, List<String>> { input -> input.split("[^\\p{L}]+").toList() })
                )
                .apply("Filter empty words", Filter.by(SerializableFunction<String, Boolean> { input -> !input.isEmpty() }))
                .apply("Count words", Count.perElement<String>())
                .apply("Write results", MapElements.into(TypeDescriptors.strings())
                                .via(ProcessFunction<KV<String, Long>, String> { input -> "${input.key} : ${input.value}" })
                )
                .apply(TextIO.write().to(outputsPrefix))


        pipeline.run().waitUntilFinish()
    }

}

