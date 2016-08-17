package org.mappinganalysis.model.functions;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Triplet;
import org.apache.flink.types.NullValue;
import org.junit.Test;
import org.mappinganalysis.BasicTest;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.Preprocessing;
import org.mappinganalysis.model.functions.simcomputation.TrigramSimilarityMapper;
import org.mappinganalysis.model.functions.simcomputation.TypeSimilarityMapper;
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

  @Test
  public void geoExample() {
    String one = "Brioni (Croatia)";
    String two = "Brijuni";
    StringMetric metric =
        with(new CosineSimilarity<>())
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
        with(new CosineSimilarity<>())
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