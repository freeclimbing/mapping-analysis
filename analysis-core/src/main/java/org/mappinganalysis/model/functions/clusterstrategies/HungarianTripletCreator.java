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
    HashSet<Vertex<Long, ObjectMap>> rightSide = Sets.newHashSet();
    HashSet<Vertex<Long, ObjectMap>> leftSide = Sets.newHashSet();

    for (Vertex<Long, ObjectMap> vertex : vertexSet) {
//      LOG.info("HTC: " + vertex.toString());
      Set<String> dataSourcesList = vertex.getValue().getDataSourcesList();
      if (dataSourcesList.contains(newSource)) {
        if (dataSourcesList.size() == 1) {
          leftSide.add(vertex);
        } else {
          throw new IllegalArgumentException("Vertex " + vertex.toString() + " " +
              "has more than one data source besides new source: " + newSource);
        }
      } else {
        rightSide.add(vertex);
      }
    }

//    LOG.info("clusters: " + vertexSet.size());
//    LOG.info("right: " + rightSide.size());
//    LOG.info("left: " + leftSide.size());
//    LOG.info("lur" + Sets.union(rightSide, leftSide).size());

    /*
    When right or left side is empty, collect a triplet from the opposite side only.
     */
    if (rightSide.isEmpty()) {
      for (Vertex<Long, ObjectMap> vertex : leftSide) {
        out.collect(new Triplet<>(
            vertex.getId(),
            vertex.getId(),
            vertex.getValue(),
            vertex.getValue(),
            NullValue.getInstance()));
      }
    } else if (leftSide.isEmpty()) {
      for (Vertex<Long, ObjectMap> vertex : rightSide) {
        out.collect(new Triplet<>(
            vertex.getId(),
            vertex.getId(),
            vertex.getValue(),
            vertex.getValue(),
            NullValue.getInstance()));
      }
    } else {
      for (Vertex<Long, ObjectMap> rightVertex : rightSide) {
        long srcId = rightVertex.getId();
        ObjectMap srcValue = rightVertex.getValue();

        for (Vertex<Long, ObjectMap> leftVertex : leftSide) {
          out.collect(new Triplet<>(
              leftVertex.getId(),
              srcId,
              leftVertex.getValue(),
              srcValue,
              NullValue.getInstance()));
        }
      }
    }
  }
}
