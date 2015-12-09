package org.mappinganalysis.model.functions;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Triplet;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.FlinkVertex;
import org.mappinganalysis.utils.Utils;

import java.util.Map;

/**
 * Return similarity 1f if labels of two resources are equal.
 */
public class TypeSimilarityMapper implements MapFunction<Triplet<Long, FlinkVertex, NullValue>,
    Triplet<Long, FlinkVertex, Map<String, Object>>> {
  @Override
  public Triplet<Long, FlinkVertex, Map<String, Object>> map(Triplet<Long, FlinkVertex, NullValue> triplet) throws Exception {
    Map<String, Object> srcProps = triplet.getSrcVertex().getValue().getProperties();
    String srcType = srcProps.containsKey(Utils.TYPE) ? srcProps.get(Utils.TYPE).toString() : Utils.NO_VALUE;
    Map<String, Object> trgProps = triplet.getTrgVertex().getValue().getProperties();
    String trgType = trgProps.containsKey(Utils.TYPE) ? trgProps.get(Utils.TYPE).toString() : Utils.NO_VALUE;

//    System.out.println(triplet);

    boolean isSimilar = false;
    if (!srcType.equals(Utils.NO_VALUE) && !trgType.equals(Utils.NO_VALUE)
        && !srcType.equals(Utils.MINUS_ONE) && !trgType.equals(Utils.MINUS_ONE)) {
      isSimilar = srcType.toLowerCase().equals(trgType.toLowerCase());
    }

    Map<String, Object> property = Maps.newHashMap();
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
