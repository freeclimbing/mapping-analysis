package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.graph.Vertex;
import org.apache.flink.hadoop.shaded.com.google.common.collect.ImmutableSortedSet;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Iterables;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Sets;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

import java.util.Set;
import java.util.SortedSet;

/**
 * Generate new component ids based on type affiliation and current component id.
 * If no type is found, the temporary component id will generated by using the vertex id.
 */
public class GenerateHashCcIdGroupReduceFunction implements GroupReduceFunction<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>> {

  @Override
  public void reduce(Iterable<Vertex<Long, ObjectMap>> vertices,
                     Collector<Vertex<Long, ObjectMap>> collector) throws Exception {
    Long hash = null;
    for (Vertex<Long, ObjectMap> vertex : vertices) {
      if (vertex.getValue().hasNoType(Utils.COMP_TYPE)) {
        vertex.getValue().put(Utils.HASH_CC, Utils.getHash(vertex.getId().toString()));
      } else {
        if (hash == null) {
          Set<String> types = vertex.getValue().getTypes(Utils.COMP_TYPE);
          ImmutableSortedSet<String> typeSet = ImmutableSortedSet.copyOf(types);

          hash = Utils.getHash(typeSet.toString()
              .concat(vertex.getValue().get(Utils.CC_ID).toString()));
        }
        vertex.getValue().put(Utils.HASH_CC, hash);
      }
      collector.collect(vertex);
    }
  }
}
