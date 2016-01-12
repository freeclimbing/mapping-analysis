package org.mappinganalysis;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.junit.Test;
import org.mappinganalysis.graph.ClusterComputation;
import org.mappinganalysis.graph.FlinkConnectedComponents;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.CcResultVerticesJoin;
import org.mappinganalysis.model.functions.CcVerticesCreator;
import org.mappinganalysis.utils.Utils;

import java.util.List;

/**
 * basic test class
 */
public class MappingAnalysisExampleTest {

  private static final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  @Test
  public void analysisTest() throws Exception {
    MappingAnalysisExample mappingAnalysisExample = new MappingAnalysisExample();
    Graph<Long, ObjectMap, NullValue> graph = createTestGraph();

//    final DataSet<Triplet<Long, ObjectMap, ObjectMap>> accumulatedSimValues
//        = MappingAnalysisExample.computeSimilarities(graph.getTriplets(), "combined");
//
//    // 1. time cc
//    final DataSet<Tuple2<Long, Long>> ccEdges = accumulatedSimValues.project(0, 1);
//    final DataSet<Long> ccVertices = baseVertices.map(new CcVerticesCreator());
//    FlinkConnectedComponents connectedComponents = new FlinkConnectedComponents(env);
//    final DataSet<Tuple2<Long, Long>> ccResult = connectedComponents
//        .compute(ccVertices, ccEdges, 1000);
//
//    ccResult.print();
//
//    DataSet<Vertex<Long, ObjectMap>> ccResultVertices = baseVertices
//        .join(ccResult)
//        .where(0).equalTo(0)
//        .with(new CcResultVerticesJoin());
//
//    // get new edges in components
//    DataSet<Edge<Long, NullValue>> newEdges
//        = ClusterComputation.restrictToNewEdges(graph.getEdges(),
//        ClusterComputation.computeComponentEdges(ccResultVertices));
//
//    DataSet<Triplet<Long, ObjectMap, ObjectMap>> newSimValues
//        = MappingAnalysisExample
//        .computeSimilarities(Graph.fromDataSet(baseVertices, newEdges, env)
//            .getTriplets(), "combined");
//
//    DataSet<Tuple2<Long, Long>> newSimValuesSimple = newSimValues.project(0, 1);
//    DataSet<Tuple2<Long, Long>> newCcEdges = newSimValuesSimple.union(ccEdges);
//    newCcEdges.print();
//
//    // 2. time cc
//    DataSet<Tuple2<Long, Long>> newCcResult = connectedComponents
//        .compute(ccVertices, newCcEdges, 1000);
//    newCcResult.print();
  }

  private Graph<Long, ObjectMap, NullValue> createTestGraph() {
    List<Edge<Long, NullValue>> edgeList = Lists.newArrayList();
    edgeList.add(new Edge<>(5680L, 5681L, NullValue.getInstance()));
    edgeList.add(new Edge<>(5680L, 5984L, NullValue.getInstance()));
    Graph<Long, NullValue, NullValue> tmpGraph = Graph.fromCollection(edgeList, env);

    final DataSet<Vertex<Long, ObjectMap>> baseVertices = tmpGraph.getVertices()
        .map(new MapFunction<Vertex<Long, NullValue>, Vertex<Long, ObjectMap>>() {
          @Override
          public Vertex<Long, ObjectMap> map(Vertex<Long, NullValue> vertex) throws Exception {
            ObjectMap prop = new ObjectMap();
            prop.put(Utils.LABEL, "foo");
            return new Vertex<>(vertex.getId(), prop);
          }
        });

    return Graph.fromDataSet(baseVertices, tmpGraph.getEdges(), env);
  }
}