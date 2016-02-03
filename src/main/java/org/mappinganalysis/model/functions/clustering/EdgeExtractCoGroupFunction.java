package org.mappinganalysis.model.functions.clustering;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.ObjectMap;

import java.util.HashSet;

public class EdgeExtractCoGroupFunction implements CoGroupFunction<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>, Edge<Long, NullValue>> {
  @Override
  public void coGroup(Iterable<Vertex<Long, ObjectMap>> left,
                      Iterable<Vertex<Long, ObjectMap>> right,
                      Collector<Edge<Long, NullValue>> collector) throws Exception {
    HashSet<Vertex<Long, ObjectMap>> rightSet = Sets.newHashSet(right);
    for (Vertex<Long, ObjectMap> vertexLeft : left) {
      for (Vertex<Long, ObjectMap> vertexRight : rightSet) {
        collector.collect(new Edge<>(vertexLeft.getId(),
            vertexRight.getId(), NullValue.getInstance()));
      }
    }
  }
}
