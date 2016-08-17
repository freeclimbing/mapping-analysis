package org.mappinganalysis.model.functions.merge;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.ObjectMap;

import java.util.HashSet;

/**
 * Create triplets for vertices with equal component id as a prerequisite
 * for a following similarity computation.
 */
public class TripletCreateGroupReduceFunction implements GroupReduceFunction<Vertex<Long, ObjectMap>,
    Triplet<Long, ObjectMap, NullValue>> {
  @Override
  public void reduce(Iterable<Vertex<Long, ObjectMap>> vertices,
                     Collector<Triplet<Long, ObjectMap, NullValue>> collector) throws Exception {
    HashSet<Vertex<Long, ObjectMap>> rightSet = Sets.newHashSet(vertices);
    HashSet<Vertex<Long, ObjectMap>> leftSet = Sets.newHashSet(rightSet);

    for (Vertex<Long, ObjectMap> left : leftSet) {
      for (Vertex<Long, ObjectMap> right : rightSet) {
        if ((long) right.getId() != left.getId())
          collector.collect(new Triplet<>(left.getId(), right.getId(), left.getValue(), right.getValue(),
              NullValue.getInstance()));
      }
      rightSet.remove(left);
    }
  }
}
