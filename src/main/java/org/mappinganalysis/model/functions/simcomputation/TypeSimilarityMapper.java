package org.mappinganalysis.model.functions.simcomputation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Triplet;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

/**
 * Return similarity 1f if labels of two resources are equal.
 */
public class TypeSimilarityMapper implements MapFunction<Triplet<Long, ObjectMap, NullValue>,
    Triplet<Long, ObjectMap, ObjectMap>> {
  @Override
  public Triplet<Long, ObjectMap, ObjectMap> map(Triplet<Long, ObjectMap, NullValue> triplet) throws Exception {
    ObjectMap srcProps = triplet.getSrcVertex().getValue();
    String srcType = srcProps.containsKey(Utils.TYPE_INTERN) ?
        srcProps.get(Utils.TYPE_INTERN).toString() : Utils.NO_VALUE;
    ObjectMap trgProps = triplet.getTrgVertex().getValue();
    String trgType = trgProps.containsKey(Utils.TYPE_INTERN) ?
        trgProps.get(Utils.TYPE_INTERN).toString() : Utils.NO_VALUE;

    boolean isSimilar = false;
    if (!srcType.equals(Utils.NO_VALUE) && !trgType.equals(Utils.NO_VALUE)
        && !srcType.equals(Utils.TYPE_NOT_FOUND) && !trgType.equals(Utils.TYPE_NOT_FOUND)) {
      isSimilar = srcType.toLowerCase().equals(trgType.toLowerCase());
    }

    ObjectMap property = new ObjectMap();
    property.put(Utils.TYPE_MATCH, (isSimilar) ? 1f : 0f);

    return new Triplet<>(
        triplet.getSrcVertex(),
        triplet.getTrgVertex(),
        new Edge<>(
            triplet.getSrcVertex().getId(),
            triplet.getTrgVertex().getId(),
            property));
  }
}
