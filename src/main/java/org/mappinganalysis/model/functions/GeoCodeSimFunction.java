package org.mappinganalysis.model.functions;

import com.google.common.primitives.Doubles;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Triplet;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.FlinkVertex;
import org.mappinganalysis.utils.GeoDistance;

import java.util.List;
import java.util.Map;

/**
 * Return triple including the distance between 2 geo points as edge value.
 */
public class GeoCodeSimFunction implements MapFunction<Triplet<Long, FlinkVertex, NullValue>,
        Triplet<Long, FlinkVertex, Double>> {

  @Override
  public Triplet<Long, FlinkVertex, Double> map(Triplet<Long,
      FlinkVertex, NullValue> triplet) throws Exception {
    Map<String, Object> source = triplet.getSrcVertex().getValue().getProperties();
    Map<String, Object> target = triplet.getTrgVertex().getValue().getProperties();


    double distance = GeoDistance.distance(getDouble(source.get("lat")),
        getDouble(source.get("lon")),
        getDouble(target.get("lat")),
        getDouble(target.get("lon")));

    return new Triplet<>(
        triplet.getSrcVertex(),
        triplet.getTrgVertex(),
        new Edge<>(triplet.getSrcVertex().getId(),
        triplet.getTrgVertex().getId(), distance));
  }

  private Double getDouble(Object latlon) {
    // TODO how to handle multiple values in lat/lon correctly?

    if (latlon instanceof List) {
      return Doubles.tryParse(((List) latlon).get(0).toString());
    } else {
      return Doubles.tryParse(latlon.toString());
    }
  }
}
