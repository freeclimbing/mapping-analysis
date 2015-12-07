package org.mappinganalysis.model.functions;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Triplet;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.FlinkVertex;
import org.mappinganalysis.utils.GeoDistance;
import org.mappinganalysis.utils.Utils;

import java.util.Map;

/**
 * Return triple including the distance between 2 geo points as edge value.
 */
public class GeoCodeSimFunction implements MapFunction<Triplet<Long, FlinkVertex, NullValue>,
        Triplet<Long, FlinkVertex, Map<String, Object>>> {

  @Override
  public Triplet<Long, FlinkVertex, Map<String, Object>> map(Triplet<Long,
      FlinkVertex, NullValue> triplet) throws Exception {
    Map<String, Object> source = triplet.getSrcVertex().getValue().getProperties();
    Map<String, Object> target = triplet.getTrgVertex().getValue().getProperties();

    Double distance = GeoDistance.distance(Utils.getDouble(source.get("lat")),
        Utils.getDouble(source.get("lon")),
        Utils.getDouble(target.get("lat")),
        Utils.getDouble(target.get("lon")));

    Map<String, Object> property = Maps.newHashMap();
    property.put("distance", distance);

    return new Triplet<>(
        triplet.getSrcVertex(),
        triplet.getTrgVertex(),
        new Edge<>(
            triplet.getSrcVertex().getId(),
            triplet.getTrgVertex().getId(),
            property));
  }


}
