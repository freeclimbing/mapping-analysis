package org.mappinganalysis.model.functions.clusterstrategies;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;

import java.util.HashSet;
import java.util.Set;

public class HungarianTripletCreator
    implements GroupReduceFunction<Vertex<Long,ObjectMap>, Triplet<Long, ObjectMap, NullValue>> {
  private static final Logger LOG = Logger.getLogger(HungarianTripletCreator.class);

  private String newSource;

  public HungarianTripletCreator(String newSource) {
    this.newSource = newSource;
  }

  @Override
  public void reduce(
      Iterable<Vertex<Long, ObjectMap>> vertices,
      Collector<Triplet<Long, ObjectMap, NullValue>> out) throws Exception {
    HashSet<Vertex<Long, ObjectMap>> vertexSet = Sets.newHashSet(vertices);
    HashSet<Vertex<Long, ObjectMap>> existingSide = Sets.newHashSet();
    HashSet<Vertex<Long, ObjectMap>> newSide = Sets.newHashSet();

    for (Vertex<Long, ObjectMap> vertex : vertexSet) {
      Set<String> dataSourcesList = vertex.getValue().getDataSourcesList();
      if (dataSourcesList.contains(newSource)) {
        if (dataSourcesList.size() == 1) {
          newSide.add(vertex);
        } else {
          throw new IllegalArgumentException("Vertex " + vertex.toString() + " " +
              "has more than one data source besides new source: " + newSource);
        }
      } else {
        existingSide.add(vertex);
      }
    }

    /*
    When right or left side is empty, collect a triplet from the opposite side only.
     */
    if (existingSide.isEmpty()) {
      for (Vertex<Long, ObjectMap> vertex : newSide) {
        out.collect(new Triplet<>(
            vertex.getId(),
            vertex.getId(),
            vertex.getValue(),
            vertex.getValue(),
            NullValue.getInstance()));
      }
    } else if (newSide.isEmpty()) {
      for (Vertex<Long, ObjectMap> vertex : existingSide) {
        out.collect(new Triplet<>(
            vertex.getId(),
            vertex.getId(),
            vertex.getValue(),
            vertex.getValue(),
            NullValue.getInstance()));
      }
    } else {
      for (Vertex<Long, ObjectMap> existingVertex : existingSide) {
        long srcId = existingVertex.getId();
        ObjectMap srcValue = existingVertex.getValue();

        for (Vertex<Long, ObjectMap> newVertex : newSide) {
          out.collect(new Triplet<>(
              newVertex.getId(),
              srcId,
              newVertex.getValue(),
              srcValue,
              NullValue.getInstance()));
        }
      }
    }
  }
}
