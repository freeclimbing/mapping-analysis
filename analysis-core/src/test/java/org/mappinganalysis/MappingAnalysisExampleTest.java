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
    for (org.s1ck.gdl.model.Vertex vertex : handler.getVertices()) {
      ObjectMap properties = new ObjectMap();
      properties.putAll(vertex.getProperties());
      vertexList.add(new Vertex<>(vertex.getId(), properties));
    }
    for (org.s1ck.gdl.model.Edge edge : handler.getEdges()) {
      ObjectMap map = new ObjectMap();
      // edges shall not be null
      map.putAll(edge.getProperties());
      edgeList.add(new Edge<>(edge.getSourceVertexId(),
          edge.getTargetVertexId(), map));
    }

    return Graph.fromCollection(vertexList, edgeList, env);
  }
}