package org.mappinganalysis.model.functions.simcomputation;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Triplet;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.model.EdgeObjectMapTriplet;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.simcomputation.ops.SinglePropertySimilarity;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.GeoDistance;
import org.mappinganalysis.util.Utils;
import org.simmetrics.StringMetric;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Set;

/**
 * Music edge similarity function
 */
public class MusicSimilarityFunction
    extends SimilarityFunction<Triplet<Long, ObjectMap, NullValue>, Triplet<Long, ObjectMap, ObjectMap>>
    implements Serializable {

  public MusicSimilarityFunction() {
  }

  @Override
  public Triplet<Long, ObjectMap, ObjectMap> map(Triplet<Long, ObjectMap, NullValue> triplet)
      throws Exception {

    EdgeObjectMapTriplet result = new EdgeObjectMapTriplet(triplet);
    System.out.println(result.toString());
    result.runOperation(new SinglePropertySimilarity());

    System.out.println(result.getEdge().getValue().toString());
//    Triplet<Long, ObjectMap, ObjectMap> result = addBasicLabelSimilarity(triplet);

    return result;
  }

  /**
   * add type similarity
   */
  private Triplet<Long, ObjectMap, ObjectMap> addTypeSimilarity(
      Triplet<Long, ObjectMap, ObjectMap> triplet) {
    Set<String> srcTypes = triplet.getSrcVertex().getValue().getTypes(Constants.TYPE_INTERN);
    Set<String> trgTypes = triplet.getTrgVertex().getValue().getTypes(Constants.TYPE_INTERN);

    if (Utils.hasEmptyType(srcTypes, trgTypes)) {
      triplet.getEdge()
          .getValue()
          .put(Constants.SIM_TYPE, Utils.getTypeSim(srcTypes, trgTypes));
    }

    return triplet;
  }


  /**
   * basic label similarity
   */
  private Triplet<Long, ObjectMap, ObjectMap> addBasicLabelSimilarity(
      Triplet<Long, ObjectMap, NullValue> triplet) {
    final String srcLabel = triplet.getSrcVertex().getValue().getLabel();
    final String trgLabel = triplet.getTrgVertex().getValue().getLabel();
    EdgeObjectMapTriplet resultTriplet = new EdgeObjectMapTriplet(triplet);
    StringMetric metric = Utils.getTrigramMetricAndSimplifyStrings();

    if (!srcLabel.equals(Constants.NO_LABEL_FOUND) && !trgLabel.equals(Constants.NO_LABEL_FOUND)) {
      double similarity = metric.compare(srcLabel.toLowerCase().trim(), trgLabel.toLowerCase().trim());
      BigDecimal tmpResult = new BigDecimal(similarity);
      similarity = tmpResult.setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue();

      resultTriplet.getEdge().getValue().put(Constants.SIM_TRIGRAM, similarity);

      return resultTriplet;
    } else {
      return resultTriplet;
    }
  }
}
