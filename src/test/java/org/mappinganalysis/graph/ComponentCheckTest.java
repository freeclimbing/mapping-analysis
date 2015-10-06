package org.mappinganalysis.graph;

import org.junit.Test;
import org.mappinganalysis.model.Component;
import org.mappinganalysis.model.Vertex;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Component check test
 */
public class ComponentCheckTest {

  @Test
  public void testGetComponentsWithOneToManyInstances() throws Exception {
    ComponentCheck tool = new ComponentCheck();
    createTestVerticesAndEdges(tool);
    assertEquals(2, tool.components.size());

    HashSet<Component> result = tool.getComponentsWithOneToManyInstances();
    assertEquals(1, result.size());
    for (Component component : result) {
      assertEquals(4794, component.getId());
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

  private void createTestVerticesAndEdges(ComponentCheck tool) {
    HashSet<Vertex> vertices = new HashSet<>();

    Vertex nytNewfoundland = new Vertex(4794, "http://data.nytimes.com/10919831131783165001",
        "http://data.nytimes.com/", "Newfoundland (Canada)");
    Vertex gnNewfLabra = new Vertex(4795, "http://sws.geonames.org/6354959/",
        "http://sws.geonames.org/", "Newfoundland and Labrador");
    Vertex nytLabrador = new Vertex(5680, "http://data.nytimes.com/66830295360330547131",
        "http://data.nytimes.com/", "Labrador (Canada)");
    Vertex dbpLabrador = new Vertex(5681, "http://dbpedia.org/resource/Labrador",
        "http://dbpedia.org/", "Labrador");
    Vertex fbLabrador = new Vertex(5984, "http://rdf.freebase.com/ns/en.labrador",
        "http://rdf.freebase.com/", "labrador");
    Vertex fbNewfoundland = new Vertex(6066, "http://rdf.freebase.com/ns/en.newfoundland",
        "http://rdf.freebase.com/", "newfoundland");
    Vertex dbpNewfIsland = new Vertex(6555, "http://dbpedia.org/resource/Newfoundland_%28island%29",
        "http://dbpedia.org/", "Newfoundland (island)");

    vertices.add(gnNewfLabra);
    vertices.add(nytLabrador);
    vertices.add(dbpLabrador);
    vertices.add(fbLabrador);

    for (Vertex vertex : vertices) { // fake correct component
      tool.addVertexToComponent(vertex, 4795);
    }

    vertices.add(nytNewfoundland);
    vertices.add(fbNewfoundland);
    vertices.add(dbpNewfIsland);

    for (Vertex vertex : vertices) {
      tool.addVertexToComponent(vertex, 4794);
    }

    tool.addEdge(nytNewfoundland.getId(), dbpNewfIsland.getId());
    tool.addEdge(nytNewfoundland.getId(), fbNewfoundland.getId());
    tool.addEdge(nytNewfoundland.getId(), gnNewfLabra.getId());
    tool.addEdge(nytLabrador.getId(), gnNewfLabra.getId());
    tool.addEdge(nytLabrador.getId(), dbpLabrador.getId());
    tool.addEdge(nytLabrador.getId(), fbLabrador.getId());
  }
}