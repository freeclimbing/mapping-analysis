package org.mappinganalysis.model.functions;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Triplet;
import org.apache.flink.types.NullValue;
import org.junit.Test;
import org.mappinganalysis.BasicTest;
import org.mappinganalysis.model.FlinkVertex;
import org.mappinganalysis.model.Preprocessing;
import org.simmetrics.StringMetric;
import org.simmetrics.metrics.CosineSimilarity;
import org.simmetrics.metrics.StringMetrics;
import org.simmetrics.tokenizers.Tokenizers;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.simmetrics.builders.StringMetricBuilder.with;

/**
 * similarity test
 */
public class SimilarityMapperTest extends BasicTest {

  @Test
  public void trigramSimilarityTest() throws Exception {
    Graph<Long, FlinkVertex, NullValue> graph = createSimpleGraph();
    final DataSet<Triplet<Long, FlinkVertex, NullValue>> baseTriplets = graph.getTriplets();

    DataSet<Triplet<Long, FlinkVertex, Map<String, Object>>> exactSim
      = baseTriplets
      .map(new TrigramSimilarityMapper())
      .filter(new TrigramSimilarityFilter());

    for (Triplet<Long, FlinkVertex, Map<String, Object>> triplet : exactSim.collect()) {
      if (triplet.getSrcVertex().getId() == 5680) {
        if (triplet.getTrgVertex().getId() == 5984 || triplet.getTrgVertex().getId() == 5681) {
          assertEquals(0.6324555f, triplet.getEdge().getValue().get("trigramSim"));
        } else {
          assertFalse(true);
        }
      }
    }
  }

  @Test
  public void typeSimilarityTest() throws Exception {
    Graph<Long, FlinkVertex, NullValue> graph = createSimpleGraph();
    graph = Preprocessing.applyTypePreprocessing(graph, ExecutionEnvironment.getExecutionEnvironment());

    final DataSet<Triplet<Long, FlinkVertex, NullValue>> baseTriplets = graph.getTriplets();

    baseTriplets.print();

    DataSet<Triplet<Long, FlinkVertex, Map<String, Object>>> typeSim
        = baseTriplets
        .map(new TypeSimilarityMapper())
        .filter(new TypeFilter());

    for (Triplet<Long, FlinkVertex, Map<String, Object>> triplet : typeSim.collect()) {
      System.out.println(triplet);
    }
  }

  @Test
  public void geoExample() {
    String one = "Brioni (Croatia)";
    String two = "Brijuni";
    StringMetric metric =
        with(new CosineSimilarity<String>())
            .tokenize(Tokenizers.qGram(3))
            .build();
    System.out.println(one + " ### " + two + " ### " + metric.compare(one, two));

    one = "Saba";
    two = "Saba (Netherlands Antilles)";
    System.out.println(one + " ### " + two + " ### " + metric.compare(one, two));
    one = "Hermanus";
    two = "Hermanus (South Africa)";
    System.out.println(one + " ### " + two + " ### " + metric.compare(one, two));
  }

  @Test
  /**
   * not rly a test
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
        with(new CosineSimilarity<String>())
            .tokenize(Tokenizers.qGram(3))
            .build();
    System.out.println(trigram.compare(leipzig, leipzigsachsen));
    System.out.println(trigram.compare(leipzig, leipzi));
    System.out.println(trigram.compare(leipzi, leipzigsachsen));

    System.out.println("cosine");
    StringMetric cosine = StringMetrics.cosineSimilarity();
    System.out.println(cosine.compare(leipzig, leipzigsachsen));

    StringMetric dice = StringMetrics.dice();
    System.out.println("dice");
    System.out.println(dice.compare(leipzig, leipzigsachsen));
  }

}