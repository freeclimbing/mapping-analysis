package org.mappinganalysis.model.functions.simcomputation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Triplet;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.util.Set;

/**
 * Get type similarity (and type shading similarity).
 */
@Deprecated
public class TypeSimilarityMapper implements MapFunction<Triplet<Long, ObjectMap, NullValue>,
    Triplet<Long, ObjectMap, ObjectMap>> {
  @Override
  public Triplet<Long, ObjectMap, ObjectMap> map(Triplet<Long, ObjectMap, NullValue> triplet) throws Exception {
    Set<String> srcTypes = triplet.getSrcVertex().getValue().getTypes(Constants.TYPE_INTERN);
    Set<String> trgTypes = triplet.getTrgVertex().getValue().getTypes(Constants.TYPE_INTERN);
    Triplet<Long, ObjectMap, ObjectMap> resultTriplet = SimilarityComputation.initResultTriplet(triplet);

    if (Utils.hasEmptyType(srcTypes, trgTypes)) {
      resultTriplet.getEdge()
          .getValue()
          .put(Constants.SIM_TYPE, Utils.getTypeSim(srcTypes, trgTypes));

      return resultTriplet;
    } else {
      return resultTriplet;
    }
  }
}
