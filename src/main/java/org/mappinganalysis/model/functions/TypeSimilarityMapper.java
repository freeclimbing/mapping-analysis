package org.mappinganalysis.model.functions;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Triplet;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.FlinkVertex;

import java.util.Map;

/**
 * Return similarity 1f if labels of two resources are equal.
 */
public class TypeSimilarityMapper implements MapFunction<Triplet<Long, FlinkVertex, NullValue>,
    Triplet<Long, FlinkVertex, Map<String, Object>>> {
  @Override
  public Triplet<Long, FlinkVertex, Map<String, Object>> map(Triplet<Long, FlinkVertex, NullValue> triplet) throws Exception {
    Map<String, Object> srcProps = triplet.getSrcVertex().getValue().getProperties();
    String srcType = srcProps.containsKey("type") ? srcProps.get("type").toString() : "null";
    Map<String, Object> trgProps = triplet.getTrgVertex().getValue().getProperties();
    String trgType = trgProps.containsKey("type") ? trgProps.get("type").toString() : "null";

    System.out.println(triplet);

    boolean isSimilar = false;
    if (!srcType.equals("null") && !trgType.equals("null")
        && !srcType.equals("-1") && !trgType.equals("-1")) {
      isSimilar = srcType.toLowerCase().equals(trgType.toLowerCase());
    }

    Map<String, Object> property = Maps.newHashMap();
    property.put("typeMatch", (isSimilar) ? 1f : 0f);

    return new Triplet<>(
        triplet.getSrcVertex(),
        triplet.getTrgVertex(),
        new Edge<>(
            triplet.getSrcVertex().getId(),
            triplet.getTrgVertex().getId(),
            property));
  }
}
