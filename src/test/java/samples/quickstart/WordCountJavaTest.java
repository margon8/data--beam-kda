package samples.quickstart;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;

@RunWith(JUnit4.class)
public class WordCountJavaTest {

    // Our static input data, which will comprise the initial PCollection.
    static final String[] WORDS_ARRAY = new String[] {
            "hi there", "hi", "hi sue bob",
            "hi sue", "", "bob hi"};

    static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);

    // Our static output data, which is the expected data that the final PCollection must match.
    static final String[] COUNTS_ARRAY = new String[] {
            "hi: 5", "there: 1", "sue: 2", "bob: 2"};

    // Example test that tests the pipeline's transforms.
    @Rule public TestPipeline p = TestPipeline.create();

    @Test
    @Category(ValidatesRunner.class)
    public void testCountWords() throws Exception {

        // Create a PCollection from the WORDS static input data.
        PCollection<String> input = p.apply(Create.of(WORDS)).setCoder(StringUtf8Coder.of());

        // Run ALL the pipeline's transforms (in this case, the CountWords composite transform).
        PCollection<String> output = input
                .apply(new WordCountJava.CountWordsCompositeTransform())
                .apply(MapElements.via(new WordCountJava.FormatAsTextFn()));

        // Assert that the output PCollection matches the COUNTS_ARRAY known static output data.
        PAssert.that(output).containsInAnyOrder(COUNTS_ARRAY);

        // Run the pipeline.
        p.run().waitUntilFinish();
    }
}