package org.mappinganalysis.model.functions;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Triplet;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.BasicTest;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.Preprocessing;
import org.mappinganalysis.model.functions.simcomputation.TrigramSimilarityMapper;
import org.mappinganalysis.model.functions.simcomputation.TypeSimilarityMapper;
import org.mappinganalysis.util.Utils;
import org.mappinganalysis.util.functions.filter.TypeFilter;
import org.simmetrics.StringMetric;
import org.simmetrics.metrics.CosineSimilarity;
import org.simmetrics.metrics.StringMetrics;
import org.simmetrics.tokenizers.Tokenizers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.simmetrics.builders.StringMetricBuilder.with;

/**
 * similarity test
 */
public class SimilarityMapperTest extends BasicTest {
  private static final Logger LOG = Logger.getLogger(SimilarityMapperTest.class);
  private static final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  @Test
  public void trigramSimilarityTest() throws Exception {
    Graph<Long, ObjectMap, NullValue> graph = createSimpleGraph();
    final DataSet<Triplet<Long, ObjectMap, NullValue>> baseTriplets = graph.getTriplets();

    DataSet<Triplet<Long, ObjectMap, ObjectMap>> exactSim
      = baseTriplets
      .map(new TrigramSimilarityMapper()); // filter deleted, maybe test no longer working?

    for (Triplet<Long, ObjectMap, ObjectMap> triplet : exactSim.collect()) {
      if (triplet.getSrcVertex().getId() == 5680) {
        if (triplet.getTrgVertex().getId() == 5984 || triplet.getTrgVertex().getId() == 5681) {
          assertEquals(0.6324555f, triplet.getEdge().getValue().get("trigramSim"));
        } else {
          assertFalse(true);
        }
      }
    }
  }

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

  /**
   * JDBC test, not working
   * @throws Exception
   */
  @Test
  public void typeSimilarityTest() throws Exception {
    Graph<Long, ObjectMap, NullValue> graph = createSimpleGraph();
    graph = Graph.fromDataSet(
        Preprocessing.applyTypeToInternalTypeMapping(graph),
        graph.getEdges(),
        env);

    final DataSet<Triplet<Long, ObjectMap, NullValue>> baseTriplets = graph.getTriplets();

    baseTriplets.print();

    DataSet<Triplet<Long, ObjectMap, ObjectMap>> typeSim
        = baseTriplets
        .map(new TypeSimilarityMapper())
        .filter(new TypeFilter());

    for (Triplet<Long, ObjectMap, ObjectMap> triplet : typeSim.collect()) {
      System.out.println(triplet);
    }
  }

  /**
   * not a test
   */
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
  }

}