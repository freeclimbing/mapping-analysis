package org.mappinganalysis.model.functions.simcomputation;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Triplet;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.GeoDistance;
import org.mappinganalysis.util.Utils;
import org.simmetrics.StringMetric;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Set;

/**
 * Basic edge similarity function (geographic)
 */
public class EdgeSimilarityFunction
    extends SimilarityFunction<Triplet<Long, ObjectMap, NullValue>, Triplet<Long, ObjectMap, ObjectMap>>
    implements Serializable {
  private final String usedPropertiesCombination;
  private final double maxDistInMeter;

  public EdgeSimilarityFunction(String usedPropertiesCombination, double maxDistInMeter) {
    this.usedPropertiesCombination = usedPropertiesCombination;
    this.maxDistInMeter = maxDistInMeter;
  }

  // Constants.SIM_GEO_LABEL_STRATEGY
  // Constants.DEFAULT_VALUE

  @Override
  public Triplet<Long, ObjectMap, ObjectMap> map(Triplet<Long, ObjectMap, NullValue> triplet)
      throws Exception {

    Triplet<Long, ObjectMap, ObjectMap> result = addBasicLabelSimilarity(triplet);
    result = addGeoSimilarity(result);
    if (usedPropertiesCombination.equals(Constants.GEO)) {
      result = addTypeSimilarity(result);
    }

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
   * add basic geo similarity
   */
  private Triplet<Long,ObjectMap,ObjectMap> addGeoSimilarity(
      Triplet<Long, ObjectMap, ObjectMap> triplet) {
    ObjectMap source = triplet.getSrcVertex().getValue();
    ObjectMap target = triplet.getTrgVertex().getValue();

    if (source.hasGeoPropertiesValid() && target.hasGeoPropertiesValid()) {
      Double distance = GeoDistance.distance(source.getLatitude(),
          source.getLongitude(), target.getLatitude(), target.getLongitude());

      if (distance >= maxDistInMeter) {
        triplet.getEdge().getValue().setGeoSimilarity(0D);
      } else {
        BigDecimal tmpResult = null;
        double tmp = 1D - (distance / maxDistInMeter);
        tmpResult = new BigDecimal(tmp);
        double result = tmpResult.setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue();
        triplet.getEdge().getValue().setGeoSimilarity(result);
      }
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
    Triplet<Long, ObjectMap, ObjectMap> resultTriplet = initResultTriplet(triplet);
    StringMetric metric = Utils.getTrigramMetricAndSimplifyStrings();

    if (!srcLabel.equals(Constants.NO_LABEL_FOUND) && !trgLabel.equals(Constants.NO_LABEL_FOUND)) {
      double similarity = metric.compare(srcLabel.toLowerCase().trim(), trgLabel.toLowerCase().trim());
      BigDecimal tmpResult = new BigDecimal(similarity);
      similarity = tmpResult.setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue();

      resultTriplet.getEdge().getValue().put(Constants.SIM_LABEL, similarity);

      return resultTriplet;
    } else {
      return resultTriplet;
    }
  }

  private Triplet<Long, ObjectMap, ObjectMap> initResultTriplet(
      Triplet<Long, ObjectMap, NullValue> triplet) {
    return new Triplet<>(
        triplet.getSrcVertex(),
        triplet.getTrgVertex(),
        new Edge<>(
            triplet.getSrcVertex().getId(),
            triplet.getTrgVertex().getId(),
            new ObjectMap())); // edge
  }
}
