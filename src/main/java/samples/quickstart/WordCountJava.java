package samples.quickstart;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;

public class WordCountJava {

    public static class CountWordsCompositeTransform
            extends PTransform<PCollection<String>, PCollection<KV<String,Long>>> {

        @Override
        public PCollection<KV<String, Long>> expand(PCollection<String> input) {

            PCollection<KV<String, Long>> wordCounts = input
                    .apply("Find words", FlatMapElements.into(TypeDescriptors.strings())
                            .via((String line) -> Arrays.asList(line.split("[^\\p{L}]+"))))
                    .apply("Filter empty words", Filter.by((String word) -> !word.isEmpty()))
                    .apply("Count words", Count.perElement());

            return wordCounts;

        }

    }

    public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
        @Override
        public String apply(KV<String, Long> input) {
            return input.getKey() + ": " + input.getValue();
        }
    }


    public static void main(String[] args) {
        String inputsDir = "data/*";
        String outputsPrefix = "outputs/part";

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);
        pipeline
                .apply("Read lines", TextIO.read().from(inputsDir))
                .apply("Count Composite", new CountWordsCompositeTransform())
                .apply("Format results", MapElements.via(new FormatAsTextFn()))
                .apply(TextIO.write().to(outputsPrefix));

        pipeline.run();
    }
}
