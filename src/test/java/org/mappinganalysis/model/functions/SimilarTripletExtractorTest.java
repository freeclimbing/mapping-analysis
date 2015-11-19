package org.mappinganalysis.model.functions;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Triplet;
import org.apache.flink.types.NullValue;
import org.junit.Test;
import org.mappinganalysis.BasicTest;
import org.mappinganalysis.model.FlinkVertex;
import org.simmetrics.StringDistance;
import org.simmetrics.StringMetric;
import org.simmetrics.metrics.CosineSimilarity;
import org.simmetrics.metrics.StringMetrics;
import org.simmetrics.tokenizers.Tokenizers;

import java.util.Map;

import static org.simmetrics.builders.StringMetricBuilder.with;

/**
 * similarity test
 */
public class SimilarTripletExtractorTest extends BasicTest {

  @Test
  public void similarityTest() throws Exception {
    Graph<Long, FlinkVertex, NullValue> graph = createSimpleGraph();
    final DataSet<Triplet<Long, FlinkVertex, NullValue>> baseTriplets = graph.getTriplets();

    DataSet<Triplet<Long, FlinkVertex, Map<String, Object>>> exactSim
      = baseTriplets
      .map(new SimilarTripletExtractor())
      .filter(new TripletFilter());
    exactSim.print();
  }

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