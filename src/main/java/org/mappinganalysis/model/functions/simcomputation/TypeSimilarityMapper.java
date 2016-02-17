package org.mappinganalysis.model.functions.simcomputation;

import com.google.common.primitives.Floats;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Triplet;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.TypeDictionary;
import org.mappinganalysis.utils.Utils;

/**
 * Return similarity 1f if labels of two resources are equal.
 */
public class TypeSimilarityMapper implements MapFunction<Triplet<Long, ObjectMap, NullValue>,
    Triplet<Long, ObjectMap, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(TypeSimilarityMapper.class);

  @Override
  public Triplet<Long, ObjectMap, ObjectMap> map(Triplet<Long, ObjectMap, NullValue> triplet) throws Exception {
    String srcType = triplet.getSrcVertex().getValue().containsKey(Utils.TYPE_INTERN) ?
        triplet.getSrcVertex().getValue().get(Utils.TYPE_INTERN).toString() : Utils.NO_TYPE_AVAILABLE;
    String trgType = triplet.getTrgVertex().getValue().containsKey(Utils.TYPE_INTERN) ?
        triplet.getTrgVertex().getValue().get(Utils.TYPE_INTERN).toString() : Utils.NO_TYPE_AVAILABLE;
    Triplet<Long, ObjectMap, ObjectMap> resultTriplet = SimCompUtility.initResultTriplet(triplet);

    if (isNoTypeEmpty(srcType, trgType)) {
      float similarity = srcType.toLowerCase().equals(trgType.toLowerCase()) ? 1f : 0f;
      if (Floats.compare(similarity, 0f) == 0) {
        similarity = checkTypeShadingSimilarity(srcType, trgType);
      }
      resultTriplet.getEdge().getValue().put(Utils.SIM_TYPE, similarity);

      return resultTriplet;
    } else {
      return resultTriplet;
    }
  }

  private boolean isNoTypeEmpty(String srcType, String trgType) {
    return !srcType.equals(Utils.NO_TYPE_AVAILABLE) && !trgType.equals(Utils.NO_TYPE_AVAILABLE)
        && !srcType.equals(Utils.NO_TYPE_FOUND) && !trgType.equals(Utils.NO_TYPE_FOUND);
  }

  private float checkTypeShadingSimilarity(String srcType, String trgType) {
    if (TypeDictionary.TYPE_SHADINGS.containsKey(srcType)
      && TypeDictionary.TYPE_SHADINGS.get(srcType).equals(trgType)
      || TypeDictionary.TYPE_SHADINGS.containsKey(trgType)
      && TypeDictionary.TYPE_SHADINGS.get(trgType).equals(srcType)) {
      return Utils.SHADING_TYPE_SIM;
    } else {
      return 0f;
    }
  }
}
