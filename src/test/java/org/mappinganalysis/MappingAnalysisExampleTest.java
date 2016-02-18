package org.mappinganalysis;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.model.ObjectMap;
import org.s1ck.gdl.GDLHandler;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * basic test class
 */
public class MappingAnalysisExampleTest {
  private static final Logger LOG = Logger.getLogger(MappingAnalysisExample.class);
  private static final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

//    @Test
//  public void typeGroupByAndSimSortGeoDatasetTest() throws Exception {
//    MappingAnalysisExample mappingAnalysis = new MappingAnalysisExample();
//
//  }

  // TODO
  //        LOG.info("#####");
//        LOG.info("##### FINAL OUT");
//        LOG.info("#####");
//        for (Vertex<Long, ObjectMap> vertex : typeGroupByGraph.getVertices().sortPartition(0, Order.ASCENDING).first(500).collect()) {
//          LOG.info(vertex);
//        }
//

//        LOG.info("#####");
//        LOG.info("#####");
//        LOG.info("#####");
//        for (Vertex<Long, ObjectMap> vertex : two.getVertices().sortPartition(0, Order.ASCENDING).first(500).collect()) {
//          LOG.info(vertex);
//        }

  public static Graph<Long, ObjectMap, ObjectMap> createTestGraph(GDLHandler handler) {
    List<Edge<Long, ObjectMap>> edgeList = Lists.newArrayList();
    List<Vertex<Long, ObjectMap>> vertexList = Lists.newArrayList();

    // create Gelly edges and vertices -> graph
    for (org.s1ck.gdl.model.Vertex v : handler.getVertices()) {
      vertexList.add(new Vertex<>(v.getId(), new ObjectMap(v.getProperties())));
    }
    for (org.s1ck.gdl.model.Edge edge : handler.getEdges()) {
      edgeList.add(new Edge<>(edge.getSourceVertexId(),
          edge.getTargetVertexId(),
          new ObjectMap(edge.getProperties())));
    }

    return Graph.fromCollection(vertexList, edgeList, env);
  }
}