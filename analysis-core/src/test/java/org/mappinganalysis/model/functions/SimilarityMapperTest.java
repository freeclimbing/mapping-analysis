package org.mappinganalysis.model.functions;

import com.google.common.base.CharMatcher;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.BasicTest;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.simcomputation.MeanAggregationFunction;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.util.Utils;
import org.simmetrics.StringMetric;
import org.simmetrics.metrics.CosineSimilarity;
import org.simmetrics.metrics.StringMetrics;
import org.simmetrics.tokenizers.Tokenizers;

import static org.junit.Assert.assertEquals;
import static org.simmetrics.builders.StringMetricBuilder.with;

/**
 * similarity test
 */
public class SimilarityMapperTest extends BasicTest {
  private static final Logger LOG = Logger.getLogger(SimilarityMapperTest.class);
  private static final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  /**
   * check simmetrics metric, check for utf8 support
   * @throws Exception
   */
  @Test
  public void easyTrigramTest() throws Exception {
    StringMetric metric = Utils.getTrigramMetricAndSimplifyStrings();

    String one = "Long Island (NY)";
    String two = "long island, NY";
    assertEquals(1, metric.compare(one, two), 0.00001);

    String specialChars = "Pułaczów";
    String rmSpecialChars = "Puaczw"; // missing special chars
    String normalChars = "Pulaczow";
    assertEquals(0.4, metric.compare(specialChars, normalChars), 0.00001);
    assertEquals(0.4472136, metric.compare(specialChars, rmSpecialChars), 0.00001);

    String chinese = "安市";
    String switchedChinese = "市安";
    String mix = "ł市"; // replaced first char
    assertEquals(0.25, metric.compare(chinese, mix), 0.00001);
    assertEquals(0, metric.compare(chinese, switchedChinese), 0.00001);


    String arabic1 = "ﻚﻓﺭ ﺐﻬﻣ";
    String arabic2 = "ﻚﻓﺭ ﺐﻬ"; // last char missing
    assertEquals(0.6681531, metric.compare(arabic1, arabic2), 0.00001);
  }

  @Test
  public void doubleValueTest() throws Exception {
    String graphPath = SimilarityMapperTest.class
        .getResource("/data/preprocessing/general/").getFile();
    ObjectMap edgeValue = new JSONDataSource(graphPath, true, env)
        .getGraph()
        .getEdges()
        .collect()
        .iterator().next().getValue()
        .runOperation(new MeanAggregationFunction());

    double weightedAggSim = SimilarityComputation.getWeightedAggSim(edgeValue);
    double mean = edgeValue.getEdgeSimilarity();

    assertEquals(0.5459223d, weightedAggSim, 0.00001);
    assertEquals(0.699052d, mean, 0.00001);
  }

  /**
   * not a test
   */
  @Test
  public void differentSimilaritiesTest() {
    String leipzig = "Leipzig";
    String leipzigsachsen = "Leipzig (Sachsen)";
    String leipzi = "Leipzi";

    System.out.println("jaro");
    StringMetric jaro = StringMetrics.jaro();
    System.out.println(jaro.compare(leipzig, leipzigsachsen));

    System.out.println(jaro.compare(leipzig, leipzi));
    System.out.println(jaro.compare(leipzi, leipzigsachsen));

    System.out.println("trigram");
    StringMetric trigram =
        with(new CosineSimilarity<>())
            .tokenize(Tokenizers.qGram(3))
            .build();
    System.out.println(trigram.compare(leipzig, leipzigsachsen));
    System.out.println(trigram.compare(leipzig, leipzi));
    System.out.println(trigram.compare(leipzi, leipzigsachsen));

    System.out.println("trigram2");
    StringMetric trigram2 =
        with(new CosineSimilarity<>())
            .tokenize(Tokenizers.qGramWithPadding(3))
            .build();
    System.out.println(trigram2.compare(leipzig, leipzigsachsen));
    System.out.println(trigram2.compare(leipzig, leipzi));
    System.out.println(trigram2.compare(leipzi, leipzigsachsen));

    System.out.println("cosine");
    StringMetric cosine = StringMetrics.cosineSimilarity();
    System.out.println(cosine.compare(leipzig, leipzigsachsen));

    StringMetric dice = StringMetrics.dice();
    System.out.println("dice");
    System.out.println(dice.compare(leipzig, leipzigsachsen));

    String song1 = "Uriah Heep - Southern Star Into the Wild";
    String song2 = "0B1-Southern Star Heep Uriah Into the Wild (2011)";
    String song3 = "Southern Star - Into the Wild Uriah Heep";
    String song4a = "Southern Star Into the Wild";
    String song4b = "Star Southern Into the Wild";
    String song5 = "Southern Star (Into the Wild) Uriah Heep Into the Wild";

    System.out.println("\nSwitch words:");
    System.out.println("#############");
    System.out.println(trigram.compare(song4a, song4b));
    System.out.println(dice.compare(song4a, song4b));
    System.out.println(cosine.compare(song4a, song4b));
    System.out.println(jaro.compare(song4a, song4b));

    System.out.println("\n song1 - song2:");
    System.out.println("#############");
    System.out.println(trigram.compare(song1, song2));
    System.out.println(dice.compare(song1, song2));
    System.out.println(cosine.compare(song1, song2));
    System.out.println(jaro.compare(song1, song2));

    System.out.println("\n others:");
    System.out.println("#############");
    System.out.println(dice.compare(song2, song3));
    System.out.println(cosine.compare(song2, song3));
    System.out.println(dice.compare(song4a, song3));
    System.out.println(cosine.compare(song4a, song3));
    System.out.println(dice.compare(song4a, song5));
    System.out.println(cosine.compare(song4a, song5));
  }

  // not a test
  @Test
  public void trimTest() {
    String test = "10,                22           ";
    String result = CharMatcher.WHITESPACE.trimFrom(test);
    result = result.replaceAll("\\s+", " ");
    System.out.println(result);
  }

}