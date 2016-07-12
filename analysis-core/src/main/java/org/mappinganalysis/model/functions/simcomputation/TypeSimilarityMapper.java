package org.mappinganalysis.model.functions.simcomputation;

import com.google.common.primitives.Doubles;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Triplet;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.TypeDictionary;

import java.util.Set;

/**
 * Get type similarity (and type shading similarity).
 */
public class TypeSimilarityMapper implements MapFunction<Triplet<Long, ObjectMap, NullValue>,
    Triplet<Long, ObjectMap, ObjectMap>> {
  @Override
  public Triplet<Long, ObjectMap, ObjectMap> map(Triplet<Long, ObjectMap, NullValue> triplet) throws Exception {
    Set<String> srcTypes = triplet.getSrcVertex().getValue().getTypes(Constants.TYPE_INTERN);
    Set<String> trgTypes = triplet.getTrgVertex().getValue().getTypes(Constants.TYPE_INTERN);
    Triplet<Long, ObjectMap, ObjectMap> resultTriplet = SimilarityComputation.initResultTriplet(triplet);

    if (hasNoEmptyType(srcTypes, trgTypes)) {
      resultTriplet.getEdge().getValue().put(Constants.SIM_TYPE, getTypeSim(srcTypes, trgTypes));

      return resultTriplet;
    } else {
      return resultTriplet;
    }
  }

  /**
   * Get type similarity for two sets of type strings, indirect type shading sim is also computed.
   * TODO rework if type shading sim is <1
   */
  private double getTypeSim(Set<String> srcTypes, Set<String> trgTypes) {
    for (String srcType : srcTypes) {
      if (trgTypes.contains(srcType)) {
        return 1;
      } else {
        for (String trgType : trgTypes) {
          double check = checkTypeShadingSimilarity(srcType, trgType);
          if (Doubles.compare(check, 0) != 0) {
            return check;
          }
        }
      }
    }
    return 0;
  }

  private boolean hasNoEmptyType(Set<String> srcType, Set<String> trgType) {
    return !srcType.contains(Constants.NO_TYPE) && !trgType.contains(Constants.NO_TYPE);
  }

  /**
   * return double because of option to reduce the result value according to shading type sim (default: 1)
   */
  private double checkTypeShadingSimilarity(String srcType, String trgType) {
    if (TypeDictionary.TYPE_SHADINGS.containsKey(srcType)
      && TypeDictionary.TYPE_SHADINGS.get(srcType).equals(trgType)
      || TypeDictionary.TYPE_SHADINGS.containsKey(trgType)
      && TypeDictionary.TYPE_SHADINGS.get(trgType).equals(srcType)) {
      return Constants.SHADING_TYPE_SIM;
    } else {
      return 0d;
    }
  }
}
