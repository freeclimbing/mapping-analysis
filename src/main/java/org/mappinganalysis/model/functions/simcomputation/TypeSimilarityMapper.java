package org.mappinganalysis.model.functions.simcomputation;

import com.google.common.primitives.Floats;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
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
    ObjectMap srcProps = triplet.getSrcVertex().getValue();
    String srcType = srcProps.containsKey(Utils.TYPE_INTERN) ?
        srcProps.get(Utils.TYPE_INTERN).toString() : Utils.NO_VALUE;
    ObjectMap trgProps = triplet.getTrgVertex().getValue();
    String trgType = trgProps.containsKey(Utils.TYPE_INTERN) ?
        trgProps.get(Utils.TYPE_INTERN).toString() : Utils.NO_VALUE;

    float similarity = 0f;
    if (!srcType.equals(Utils.NO_VALUE) && !trgType.equals(Utils.NO_VALUE)
        && !srcType.equals(Utils.TYPE_NOT_FOUND) && !trgType.equals(Utils.TYPE_NOT_FOUND)) {
      similarity = srcType.toLowerCase().equals(trgType.toLowerCase()) ? 1f : 0f;
    }
    if (Floats.compare(similarity, 0f) == 0) {
      if (TypeDictionary.TYPE_SHADINGS.containsKey(srcType)
        && TypeDictionary.TYPE_SHADINGS.get(srcType).equals(trgType)
        || TypeDictionary.TYPE_SHADINGS.containsKey(trgType)
        && TypeDictionary.TYPE_SHADINGS.get(trgType).equals(srcType)) {
        similarity = 0.8f;
      }
    }

    ObjectMap propertyMap = new ObjectMap();
    propertyMap.put(Utils.TYPE_MATCH, similarity);

    return new Triplet<>(
        triplet.getSrcVertex(),
        triplet.getTrgVertex(),
        new Edge<>(
            triplet.getSrcVertex().getId(),
            triplet.getTrgVertex().getId(),
            propertyMap));
  }
}
