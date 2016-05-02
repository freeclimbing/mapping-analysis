/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.mappinganalysis.io.output;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

import java.util.List;

/**
 * Operator deriving a string representation from a graph collection.
 * The representation follows the concept of a canonical adjacency matrix.
 */
public class CanonicalAdjacencyMatrixBuilder {
  public <T> DataSet<String> executeOnTuples(DataSet<T> tuple) {

    DataSet<String> tupleStrings = tuple
        .map(new MapFunction<T, String>() {
          @Override
          public String map(T t) throws Exception {
            return t.toString();
          }
        });

    return tupleStrings.reduceGroup(new ConcatStrings());
  }

  public DataSet<String> executeOnVertices(DataSet<Vertex<Long, ObjectMap>> vertices) {

    DataSet<VertexString> vertexLabels = vertices
        .flatMap(new FlatMapFunction<Vertex<Long, ObjectMap>, VertexString>() {
          @Override
          public void flatMap(
              Vertex<Long, ObjectMap> vertex, Collector<VertexString> collector) throws Exception {
            Long vertexId = vertex.getId();
            String vertexLabel = "(" + vertex.getValue().get(Utils.LABEL) + ")";
            if (vertex.getValue().containsKey(Utils.CL_VERTICES)) {
              vertexLabel = vertexLabel.concat(vertex.getValue().getVerticesList().toString());
            }

            collector.collect(new VertexString(vertexId, vertexLabel));
          }
        });

    return vertexLabels
        .reduceGroup(new ConcatVertexStrings());
  }

  /**
   *
   * @param vertices cluster vertices
   * @param baseVertices base vertices
   * @return
   */
  public DataSet<String> executeOnRandomFinalClusterBaseVertexValues(DataSet<Vertex<Long, ObjectMap>> vertices,
                                                                     DataSet<Vertex<Long, ObjectMap>> baseVertices) {

    DataSet<Tuple3<Long, String, Long>> tuple3 = vertices
        .flatMap(new FlatMapFunction<Vertex<Long, ObjectMap>, Tuple3<Long, String, Long>>() {
          @Override
          public void flatMap(
              Vertex<Long, ObjectMap> vertex, Collector<Tuple3<Long, String, Long>> collector) throws Exception {
            String vertexLabel = vertex.getValue().get(Utils.LABEL).toString();
            if (vertex.getValue().containsKey(Utils.CL_VERTICES)) {
              for (Long clVertex : vertex.getValue().getVerticesList()) {
                collector.collect(new Tuple3<>(vertex.getId(), vertexLabel, clVertex));
              }
            } else {
              collector.collect(new Tuple3<>(vertex.getId(),
                  vertexLabel, (long) vertex.getValue().get(Utils.CC_ID)));
            }
          }
        });

    DataSet<VertexLabelString> vertexLabelStrings = tuple3
        .leftOuterJoin(baseVertices)
        .where(2).equalTo(0)
        .with(new FlatJoinFunction<Tuple3<Long, String, Long>, Vertex<Long, ObjectMap>, VertexLabelString>() {
          @Override
          public void join(Tuple3<Long, String, Long> left, Vertex<Long, ObjectMap> right,
                           Collector<VertexLabelString> collector) throws Exception {
            if (left != null) {
              String value = Utils.toString(right);
              collector.collect(new VertexLabelString(left.f0, left.f1, value));
            }
          }
        });

    return vertexLabelStrings
        .reduceGroup(new ConcatVertexLabelStrings());
  }

  /**
   * @param finalVertices
   * @param randomBaseVertices cluster randomBaseVertices
   * @return
   */
  public DataSet<String> executeOnVertices2(DataSet<Vertex<Long, ObjectMap>> randomBaseVertices,
                                            DataSet<Vertex<Long, ObjectMap>> finalVertices) {

    DataSet<Tuple1<Long>> clusterIds = randomBaseVertices
        .map(new MapFunction<Vertex<Long, ObjectMap>, Tuple1<Long>>() {
          @Override
          public Tuple1<Long> map(Vertex<Long, ObjectMap> vertex) throws Exception {
            return new Tuple1<>(vertex.getId());
          }
        })
        .distinct();

    DataSet<Tuple2<Long, Long>> options = finalVertices
        .flatMap(new FlatMapFunction<Vertex<Long, ObjectMap>, Tuple2<Long, Long>>() {
          @Override
          public void flatMap(Vertex<Long, ObjectMap> vertex,
                              Collector<Tuple2<Long, Long>> collector) throws Exception {
            if (vertex.getValue().containsKey(Utils.CL_VERTICES)) {
              for (Long vertexListValue : vertex.getValue().getVerticesList()) {
                collector.collect(new Tuple2<>(vertex.getId(), vertexListValue));
              }
            }
          }
        });

    DataSet<Tuple2<Long, Long>> vertexContainedInFinal = clusterIds
        .leftOuterJoin(options) // comp vertex id / contained vertex
        .where(0).equalTo(1)
        .with(new FlatJoinFunction<Tuple1<Long>, Tuple2<Long, Long>, Tuple2<Long, Long>>() {
          @Override
          public void join(Tuple1<Long> left, Tuple2<Long, Long> right,
                           Collector<Tuple2<Long, Long>> collector) throws Exception {
            if (left != null) {
              collector.collect(new Tuple2<>(left.f0, right.f0));
            }
          }
        });

    DataSet<VertexLabelString> vertexLabelStrings = vertexContainedInFinal
        .leftOuterJoin(randomBaseVertices)
        .where(0).equalTo(0)
        .with(new FlatJoinFunction<Tuple2<Long, Long>, Vertex<Long, ObjectMap>, VertexLabelString>() {
          @Override
          public void join(Tuple2<Long, Long> left, Vertex<Long, ObjectMap> right,
                           Collector<VertexLabelString> collector) throws Exception {
            if (left != null) {
              Long clusterId = (long) right.getValue().get(Utils.CC_ID);
              String value = Utils.toString(right, left.f1);
              collector.collect(new VertexLabelString(clusterId, clusterId.toString(), value));
            }
          }
        });

    return vertexLabelStrings
        .reduceGroup(new ConcatVertexLabelStrings());
  }

  public <T> DataSet<String> execute(Graph<Long, ObjectMap, T> graph) {
    // label vertices
    DataSet<VertexString> vertexLabels = graph.getVertices()
        .flatMap(new FlatMapFunction<Vertex<Long, ObjectMap>, VertexString>() {
          @Override
          public void flatMap(
              Vertex<Long, ObjectMap> vertex, Collector<VertexString> collector) throws Exception {
            Long vertexId = vertex.getId();
            String vertexLabel = "(" + vertex.getValue().get(Utils.LABEL) + ")";

            collector.collect(new VertexString(vertexId, vertexLabel));

          }
        });

    // label edges
    DataSet<EdgeString> edgeLabels = graph.getEdges()
        .flatMap(new FlatMapFunction<Edge<Long, T>, EdgeString>() {
          @Override
          public void flatMap(Edge<Long, T> edge, Collector<EdgeString> collector) throws Exception {
            Long sourceId = edge.getSource();
            Long targetId = edge.getTarget();

            collector.collect(new EdgeString(sourceId, targetId));
          }
        });

    // extend edge labels by vertex labels
    edgeLabels = edgeLabels
        .join(vertexLabels)
        .where(0).equalTo(0) //sourceId = vertexId
        .with(new SourceStringUpdater())
        .join(vertexLabels)
        .where(1).equalTo(0) //targetId = vertexId
        .with(new TargetStringUpdater());

    // extend vertex labels by outgoing vertex+edge labels
    DataSet<VertexString> outgoingAdjacencyListLabels = edgeLabels
        .groupBy(0) // graphId, sourceId
        .reduceGroup(new OutgoingAdjacencyList());


    // extend vertex labels by outgoing vertex+edge labels

    DataSet<VertexString> incomingAdjacencyListLabels = edgeLabels
        .groupBy(1) // graphId, targetId
        .reduceGroup(new IncomingAdjacencyList());

    // combine vertex labels
    vertexLabels = vertexLabels
        .leftOuterJoin(outgoingAdjacencyListLabels)
        .where(0, 1).equalTo(0, 1)
        .with(new LabelCombiner())
        .leftOuterJoin(incomingAdjacencyListLabels)
        .where(0, 1).equalTo(0, 1)
        .with(new LabelCombiner());

    return vertexLabels.reduceGroup(new ConcatVertexStrings());
  }

  private class LabelCombiner implements JoinFunction<VertexString, VertexString, VertexString> {

    @Override
    public VertexString join(VertexString left, VertexString right) throws Exception {
      String rightLabel = right == null ? "" : right.getLabel();

      left.setLabel(left.getLabel() + rightLabel);

      return left;    }
  }
}
