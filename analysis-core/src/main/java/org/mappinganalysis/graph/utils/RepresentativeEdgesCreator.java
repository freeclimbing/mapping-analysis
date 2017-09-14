package org.mappinganalysis.graph.utils;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;

import java.util.HashSet;

public class RepresentativeEdgesCreator
    implements CustomUnaryOperation<Vertex<Long, ObjectMap>, Edge<Long, NullValue>> {
  private static final Logger LOG = Logger.getLogger(SimpleEdgesCreator.class);

  private DataSet<Vertex<Long, ObjectMap>> vertices;

  @Override
  public void setInput(DataSet<Vertex<Long, ObjectMap>> vertices) {
    this.vertices = vertices;
  }

  @Override
  public DataSet<Edge<Long, NullValue>> createResult() {
    return vertices
        .flatMap(new FlatMapFunction<Vertex<Long, ObjectMap>, Edge<Long, NullValue>>() {
          @Override
          public void flatMap(Vertex<Long, ObjectMap> vertex,
                              Collector<Edge<Long, NullValue>> out) throws Exception {
            HashSet<Long> rightVerticesList = Sets.newHashSet(
                vertex.getValue().getVerticesList());
            HashSet<Long> leftVerticesList = Sets.newHashSet(rightVerticesList);
            for (Long left : leftVerticesList) {
              rightVerticesList.remove(left);
              for (Long right : rightVerticesList) {
                out.collect(new Edge<>(left, right, NullValue.getInstance()));
              }
            }
          }
        });
  }
}