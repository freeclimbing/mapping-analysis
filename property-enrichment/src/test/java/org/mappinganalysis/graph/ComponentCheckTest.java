package org.mappinganalysis.graph;

import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;
import org.mappinganalysis.model.Component;
import org.mappinganalysis.model.CompCheckVertex;
import org.mappinganalysis.util.Utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.HashSet;

import static org.junit.Assert.*;

/**
 * Component check test
 */
public class ComponentCheckTest {

  @Test
  public void testGetComponentsWithOneToManyInstances() throws Exception {
    ComponentCheck tool = new ComponentCheck("", Utils.GEO_PERFECT_DB_NAME);
    createTestVerticesAndEdges(tool);
    assertEquals(2, tool.getComponents().size());

    HashSet<Component> result = tool.getComponentsWithOneToManyInstances();
    assertEquals(1, result.size());
    for (Component component : result) {
      assertEquals(4794, component.getId());
      assertEquals(7, component.getVertices().size());
    }
  }

//  @Test
//  public void testTraverse() throws Exception {
//    ComponentCheck tool = new ComponentCheck();
//    createTestVerticesAndEdges(tool);
//
//    for (Component component : tool.components) {
//      HashSet<String> uniqueOntologies = new HashSet<>();
//
//      for (Vertex vertex : component.getVertices()) {
//        LinkedHashSet<Vertex> processed = new LinkedHashSet<>();
//        tool.traverse(component, uniqueOntologies, vertex, processed);
//
//      }
//    }
//
//  }

  /**
   * Create test data for testGetComponentsWithOneToManyInstances
   * @param tool tool
   */
  private void createTestVerticesAndEdges(ComponentCheck tool) {
    HashSet<CompCheckVertex> vertices = new HashSet<>();

    CompCheckVertex nytNewfoundland = new CompCheckVertex(4794, "http://data.nytimes.com/10919831131783165001",
        "http://data.nytimes.com/", "Newfoundland (Canada)");
    CompCheckVertex gnNewfLabra = new CompCheckVertex(4795, "http://sws.geonames.org/6354959/",
        "http://sws.geonames.org/", "Newfoundland and Labrador");
    CompCheckVertex nytLabrador = new CompCheckVertex(5680, "http://data.nytimes.com/66830295360330547131",
        "http://data.nytimes.com/", "Labrador (Canada)");
    CompCheckVertex dbpLabrador = new CompCheckVertex(5681, "http://dbpedia.org/resource/Labrador",
        "http://dbpedia.org/", "Labrador");
    CompCheckVertex fbLabrador = new CompCheckVertex(5984, "http://rdf.freebase.com/ns/en.labrador",
        "http://rdf.freebase.com/", "labrador");
    CompCheckVertex fbNewfoundland = new CompCheckVertex(6066, "http://rdf.freebase.com/ns/en.newfoundland",
        "http://rdf.freebase.com/", "newfoundland");
    CompCheckVertex dbpNewfIsland = new CompCheckVertex(6555, "http://dbpedia.org/resource/Newfoundland_%28island%29",
        "http://dbpedia.org/", "Newfoundland (island)");

    vertices.add(gnNewfLabra);
    vertices.add(nytLabrador);
    vertices.add(dbpLabrador);
    vertices.add(fbLabrador);

    for (CompCheckVertex vertex : vertices) { // fake correct component
      tool.addVertexToComponent(vertex, 4795);
    }

    vertices.add(nytNewfoundland);
    vertices.add(fbNewfoundland);
    vertices.add(dbpNewfIsland);

    for (CompCheckVertex vertex : vertices) {
      tool.addVertexToComponent(vertex, 4794);
    }

    tool.addEdge(nytNewfoundland.getId(), dbpNewfIsland.getId());
    tool.addEdge(nytNewfoundland.getId(), fbNewfoundland.getId());
    tool.addEdge(nytNewfoundland.getId(), gnNewfLabra.getId());
    tool.addEdge(nytLabrador.getId(), gnNewfLabra.getId());
    tool.addEdge(nytLabrador.getId(), dbpLabrador.getId());
    tool.addEdge(nytLabrador.getId(), fbLabrador.getId());
  }

  @Test
  public void testCreateDbAndFlinkVertices() throws Exception {
    Connection connection = Utils.openDbConnection(Utils.GEO_PERFECT_DB_NAME);
    ComponentCheck componentCheck = new ComponentCheck("strategy-exclude", Utils.GEO_PERFECT_DB_NAME);
    ResultSet resLabels = componentCheck.getLabels(connection);
    componentCheck.setLabels(resLabels);

    HashSet<Integer> flinkVertices = componentCheck.createFlinkVertices(connection);
    assertEquals(7540, componentCheck.getVerticesCount());
    assertEquals(7540, flinkVertices.size());

    HashSet<Tuple2<Integer, Integer>> flinkEdges = componentCheck.createDbAndFlinkEdges(connection);
    assertEquals(5627, componentCheck.getEdgeCount());
    assertEquals(5613, componentCheck.getEdges().size());
    assertEquals(5613, flinkEdges.size());
  }

  @Test
  public void testGetVertex() throws Exception {

  }

  @Test
  public void testGetComponentsWithOneToManyInstances1() throws Exception {

  }

  @Test
  public void testTraverse() throws Exception {

  }

  @Test
  public void testAddEdge() throws Exception {

  }

  @Test
  public void testAddVertexToComponent() throws Exception {

  }
}