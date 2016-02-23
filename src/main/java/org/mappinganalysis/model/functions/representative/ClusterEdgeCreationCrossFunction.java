package org.mappinganalysis.model.functions.representative;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.GeoDistance;
import org.mappinganalysis.utils.Utils;
import org.simmetrics.StringMetric;

import java.util.Map;
import java.util.Set;

/**
 * Compute intensive cluster link cross function.
 */
public class ClusterEdgeCreationCrossFunction implements CrossFunction<Vertex<Long, ObjectMap>,
    Vertex<Long, ObjectMap>, Edge<Long, Double>> {
  @Override
  public Edge<Long, Double> cross(Vertex<Long, ObjectMap> left,
                                  Vertex<Long, ObjectMap> right) throws Exception {
    double similarity;
    if ((long) left.getId() == right.getId() || left.getId() > right.getId()) {
      similarity = 0;
    } else {
      similarity = labelSim(left, right) * 0.4
          + geoSim(left, right, 20) * 0.4
          + typeSim(left, right) * 0.2;
    }
    return new Edge<>(left.getId(), right.getId(), similarity);
  }

  private Double typeSim(Vertex<Long, ObjectMap> left, Vertex<Long, ObjectMap> right) {
    if (left.getValue().containsKey(Utils.TYPE) && right.getValue().containsKey(Utils.TYPE)) {
      if (left.getValue().get(Utils.TYPE).equals(right.getValue().get(Utils.TYPE))) {
        return 1.0;
      }
    }
    return 0.0;
  }

  /**
   * Compute geo coordinate similarity, norm it to a distance given in advance.
   *
   * @param left    left vertex
   * @param right   right vertex
   * @param maxDist max distance (in km) which is used for normalization
   * @return normalized distance value
   */
  private Double geoSim(Vertex<Long, ObjectMap> left, Vertex<Long, ObjectMap> right, int maxDist) {
    if (left.getValue().containsKey(Utils.LON) && left.getValue().containsKey(Utils.LAT)
        && right.getValue().containsKey(Utils.LON) && right.getValue().containsKey(Utils.LAT)) {
      Map<String, Object> source = left.getValue();
      Map<String, Object> target = right.getValue();

      Double distance = GeoDistance.distance(Utils.getDouble(source.get(Utils.LAT)),
          Utils.getDouble(source.get(Utils.LON)),
          Utils.getDouble(target.get(Utils.LAT)),
          Utils.getDouble(target.get(Utils.LON)));
      return distance > maxDist * 1000 ? 0 : (maxDist - distance) / maxDist;
    }
    return 0.0;
  }

  /**
   * Get highest label similarity of two vertices, both of them have either single label or set of labels.
   *
   * @param left  left vertex
   * @param right right vertex
   * @return trigram similarity value
   */
  private Double labelSim(Vertex<Long, ObjectMap> left, Vertex<Long, ObjectMap> right) {
    // TODO check if basic trigram metric is enough here
    StringMetric metric = Utils.getBasicTrigramMetric();
    double sim = 0;

    if (left.getValue().containsKey(Utils.LABEL) && right.getValue().containsKey(Utils.LABEL)) {
      Object leftLabel = left.getValue().get(Utils.LABEL);
      Set<String> leftValues = null;
      Object rightLabel = right.getValue().get(Utils.LABEL);
      Set<String> rightValues = null;
      if (leftLabel instanceof Set) {
        leftValues = Sets.newHashSet((Set<String>) leftLabel);
      }
      if (rightLabel instanceof Set) {
        rightValues = Sets.newHashSet((Set<String>) rightLabel);
      }

      if (leftValues != null) {
        for (String lValue : leftValues) {
          if (lValue != null) {
            sim = Math.max(sim, getSimForSingleLeftValue(metric, rightLabel, rightValues, lValue));
          }
        }
      } else {
        sim = Math.max(sim, getSimForSingleLeftValue(metric, rightLabel, rightValues, (String) leftLabel));
      }
    }
    return sim < 0.5 ? 0 : sim;
  }

  private Double getSimForSingleLeftValue(StringMetric metric, Object rightLabel, Set<String> rightValues, String lValue) {
    if (rightValues != null) {
      double tmpSim = 0;
      for (String rValue : rightValues) {
        if (rValue != null) {
          tmpSim = Math.max(tmpSim, metric.compare(rValue, lValue));
        }
      }
      return tmpSim;
    } else {
      return rightLabel != null ? (double) metric.compare((String) rightLabel, lValue) : 0;
    }
  }
}
