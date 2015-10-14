package org.mappinganalysis.graph;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.Component;
import org.mappinganalysis.model.Vertex;
import org.mappinganalysis.utils.HaversineGeoDistance;
import org.mappinganalysis.utils.Utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Check components for 1:n links, dissolve into single components.
 */
public class ComponentCheck {
  private static final Logger LOG = Logger.getLogger(ComponentCheck.class);

  private static final String STRATEGY_EXCLUDE = "strategy-exclude";

  HashSet<Component> components = new HashSet<>();
  HashMap<Integer, String> labels = new HashMap<>();
  HashSet<Pair> edges = new HashSet<>();
  String strategy = "";


  public ComponentCheck(String strategy) {
    this.strategy = strategy;
  }

  public static void main(String[] args) throws Exception {
    BasicConfigurator.configure();

    Connection connection = Utils.openDbConnection(Utils.GEO_PERFECT_DB_NAME);
    ComponentCheck check = new ComponentCheck(STRATEGY_EXCLUDE);

    check.populateComponents(connection);

    System.out.println("Old size: " + check.components.size());

//    HashSet<Component> compCopy = new HashSet<>(check.components);
    HashSet<Component> errorComponents = check.getComponentsWithOneToManyInstances();
    for (Component component : errorComponents) {
      check.components.remove(component);
    }

    System.out.println("New size: " + check.components.size());

    int count = 0;
    for (Component component : check.components) {
      if (count > 10) {
        break;
      }
      System.out.println("Component: " + component.getId());
      if (simpleCompare(component.getVertices())) {
        ++count;
      }
    }
    System.out.println("complete components: " + count);

    printStats(check);

//    check.process();


//    worker.printComponents();
  }

  /**
   * Print statistics from the dataset.
   * @param check process
   */
  private static void printStats(ComponentCheck check) {
    int vertexCount = 0;
    int nytCount = 0;
    int missingTypeCount = 0;
    int missingLatLonCount = 0;
    int missingBothCount = 0;
    for (Component component : check.components) {
      for (Vertex vertex : component.getVertices()) {
        ++vertexCount;
        boolean isNyt = Boolean.FALSE;
        if (vertex.getSource().startsWith("http://data.nyt")) {
          ++nytCount;
          isNyt = Boolean.TRUE;
        }
        boolean bothMissingPremise = Boolean.FALSE;
        if (!isNyt && vertex.getTypeSet().isEmpty()) {
//          System.out.println(vertex.getUrl());
          ++missingTypeCount;
          bothMissingPremise = Boolean.TRUE;
        }
        if (vertex.getLat() == 0 || vertex.getLon() == 0) {
          ++missingLatLonCount;
//          System.out.println(vertex.getUrl());
          if (bothMissingPremise) {
            ++missingBothCount;
          }
        }
      }
    }
    System.out.println("#########################");
    System.out.println("Vertex Count: " + vertexCount);
    System.out.println("NYT resources: " + nytCount);
    System.out.println("Missing type (nyt resources are excluded here, no type available): " + missingTypeCount);
    System.out.println("##########################");
    System.out.println("Missing lat/lon: " + missingLatLonCount);
    System.out.println("Missing both: " + missingBothCount);
  }

  private static boolean simpleCompare(HashSet<Vertex> vertices) {
    double lat = 0;
    double lon = 0;
    for (Vertex vertex : vertices) {
      if (vertex.getLat() == 0.0 || vertex.getLon() == 0.0) {
        return false;
      } else if (!vertex.getSource().startsWith("http://data.nyt") && vertex.getTypeSet().isEmpty()) {
        return false;
      }
    }
    for (Vertex vertex : vertices) {
      if (lat != 0) {
        double result = HaversineGeoDistance.distance(lat, lon, vertex.getLat(), vertex.getLon());
        System.out.println("##### distance to last vertex: " + result/1000 + " km");
      }
      System.out.println(vertex.toString());
      lat = vertex.getLat();
      lon = vertex.getLon();
    }
    return true;
  }

  private void populateComponents(Connection connection) throws SQLException {
    if (strategy.equals(STRATEGY_EXCLUDE)) {

    }
    ResultSet resLabels = getLabels(connection);
    setLabels(resLabels);

    ResultSet resNodes = getNodes(connection);
    addNodesToComponents(resNodes);

    ResultSet properties = getProperties(connection);
    addProperties(properties);

    ResultSet resEdges = getEdges(connection);
    addEdges(resEdges);
  }

  /**
   * Loop through all components to check quality of contained vertices.
   */
  private void process() {
    for (Component component : components) {
      HashSet<String> uniqueOntologies = new HashSet<>();

      for (Vertex vertex : component.getVertices()) {
        LinkedHashSet<Vertex> processed = new LinkedHashSet<>();
        System.out.println("--- next vertex: " + vertex.getId());
//        traverse(component, uniqueOntologies, vertex, processed);
      }
    }
  }

  /**
   * TODO
   * @return
   */
  public HashSet<Component> getComponentsWithOneToManyInstances() {
    HashSet<Component> excludedComponents = new HashSet<>();

    for (Component component : components) {
      HashSet<String> uniqueOntologies = new HashSet<>();
      for (Vertex vertex : component.getVertices()) {
        if (!uniqueOntologies.add(vertex.getSource())) {
          excludedComponents.add(component);
          break;
        }
      }
    }

    return excludedComponents;
  }

  /**
   * TODO not yet working correctly/completely
   * Check integrity of a single component, create new components on error case.
   * Recursively processes all vertices within the component.
   * @param component component to be checked
   * @param uniqueOntologies set of (already) involved unique ontologies
   * @param vertex starting vertex for this run
   * @param processed already processed vertices
   */
  public void traverse(Component component, HashSet<String> uniqueOntologies,
    Vertex vertex, LinkedHashSet<Vertex> processed) {
    Set<Integer> neighbors = getNeighbors(edges, vertex.getId());
    System.out.println("--- next vertex: " + vertex.getId());

    if (!uniqueOntologies.add(vertex.getSource())) {
      System.out.println(vertex.getId() + ": already processed a vertex with source: " + vertex.getSource() );
      // 1. case: error found
      // * check path backwards
      // * check neighbors
      // * create new component

      Vertex dupOntVertex = findDuplicateOntologyVertex(vertex, processed);
      Vertex sourceVertex = processed.iterator().next();
    } else {
      System.out.println(vertex.getId() + ": vertex not yet processed, process... ");
      if (!neighbors.isEmpty()) {
        System.out.println(vertex.getId() + ": neighbors: " + neighbors);
        for (Integer neighbor : neighbors) {
          System.out.println(vertex.getId() + ": -- is neighbor " + neighbor + " already contained in processed vertices?");
          Vertex neighborVertex = component.getVertex(neighbor);
          if (processed.contains(neighborVertex)) {
            System.out.println(vertex.getId() + ": neighbors: " + neighbors);
            System.out.println(vertex.getId() + ": yes, neighbor already existing " + neighbor);
            // 3. case: vertex already processed
//            break;
          } else {
            // 4. process next vertex
            // Vertex next = component.getVertex(neighbor);//neighbors.iterator().next());
            processed.add(vertex);
            System.out.println(vertex.getId() + ": neighbors: " + neighbors);
            System.out.println(vertex.getId() + ": no, take this vertex (from neighbors) to process: " + neighborVertex.getId());
            traverse(component, uniqueOntologies, neighborVertex, processed);
          }
        }
      } // case: no neighbors
    }
    System.out.println(vertex.getId() + ": -- done");
  }

  /**
   * Add edges to custom edge set.
   * @param resEdges result set
   * @throws SQLException
   */
  private void addEdges(ResultSet resEdges) throws SQLException {
    while (resEdges.next()) {
      Pair pair = new Pair(resEdges.getInt(1), resEdges.getInt(2));
      edges.add(pair);
    }
  }

  /**
   * Add single edge within a component.
   * @param sourceId source vertex id
   * @param targetId target vertex id
   */
  public void addEdge(int sourceId, int targetId) {
    edges.add(new Pair(sourceId, targetId));
  }

  /**
   * Get all edges from database
   * @param connection db connection
   * @return SQL result set
   * @throws SQLException
   */
  private ResultSet getEdges(Connection connection) throws SQLException {
    String sql = "SELECT srcID, trgID FROM linksWithIDs";
    PreparedStatement s = connection.prepareStatement(sql);

    return s.executeQuery();
  }

  /**
   * Check if vertex is from source which has already been processed.
   * @param vertex vertex to be checked
   * @param processed already processed vertices
   * @return return vertex where source is equal
   */
  private Vertex findDuplicateOntologyVertex(Vertex vertex,
    LinkedHashSet<Vertex> processed) {
    for (Vertex checkVertex : processed) {
      if (checkVertex.getSource().equals(vertex.getSource())) {
        System.out.println(checkVertex.getId() + " same source like " + vertex.getId());
        return checkVertex;
      }
    }
    return null;
  }

  /**
   * Get neighbors for a given vertex
   * @param edges edges from starting vertex
   * @param id vertex id
   * @return set of vertex ids for neighbors
   */
  private Set<Integer> getNeighbors(HashSet<Pair> edges, int id) {
    HashSet<Integer> neighbors = new HashSet<>();
    for (Pair edge : edges) {
      if (edge.getSource() == id) {
        neighbors.add(edge.getTarget());
      } else if (edge.getTarget() == id) {
        neighbors.add(edge.getSource());
      }
    }
    return neighbors;
  }

  /**
   * Add properties to already existing vertices.
   * @param properties result set containing properties
   */
  private void addProperties(ResultSet properties) throws SQLException {
    while (properties.next()) {
      int id = properties.getInt(1);
      String key = properties.getString(2);
      String value = properties.getString(3);

      for (Component c : components) {
        Vertex vertex = c.getVertex(id);
        if (vertex != null) {
          switch (key) {
            case "lat":
              vertex.setLat(Double.parseDouble(value));
              break;
            case "lon":
//              if (value.endsWith(".")) {
//                System.out.println("id: " + vertex.getId() + " value: " + value);
//              } else {
                vertex.setLon(Double.parseDouble(value));
//              }
              break;
            case "type":
              vertex.addType(value);
              break;
          }
        }
      }
    }
  }

  /**
   * Add all nodes to components. If component does not exist, create it.
   * @param resNodes SQL result set of all nodes
   */
  private void addNodesToComponents(ResultSet resNodes) throws SQLException {
    while (resNodes.next()) {
      int id = resNodes.getInt(1);
      String url = resNodes.getString(2);
      String source = resNodes.getString(3);
      int ccId = resNodes.getInt(4);

      Vertex vertex = new Vertex(id, url, source, labels.get(id));
      addVertexToComponent(vertex, ccId);
    }
  }

  /**
   * Add a single vertex to its corresponding component.
   * @param vertex vertex
   * @param ccId component id
   */
  public void addVertexToComponent(Vertex vertex, int ccId) {
    if (!addToExistingComponent(vertex, ccId)) {
      Component newComponent = new Component(ccId);
      newComponent.addVertex(vertex);
      components.add(newComponent);
    }
  }

  /**
   * Try to add a single node to a component.
   * @param vertex vertex
   * @param ccId component id
   * @return true if component already exists
   */
  private boolean addToExistingComponent(Vertex vertex, int ccId) {
    for (Component tmp : components) {
      if (tmp.getId() == ccId) {
        tmp.addVertex(vertex);
        return true;
      }
    }
    return false;
  }

  /**
   * Print component information to LOG
   */
  private void printComponents() {
    for (Component component : components) {
      Set<Integer> vertices = component.getVerticesAsInt();
      Set<Vertex> v = component.getVertices();
      String componentLabels = "";
      for (Vertex vertex : v) {
        componentLabels += vertex.getLabel() + " ";
      }
      LOG.info(component.getId() + ": [" + vertices.toString() + "]");
      LOG.info(componentLabels);
    }
  }

  /**
   * Populate labels from SQL result set.
   * @param resLabels SQL result set
   * @throws SQLException
   */
  private void setLabels(ResultSet resLabels) throws SQLException {
    while (resLabels.next()) {
      labels.put(resLabels.getInt(1), resLabels.getString(2));
    }
  }

  /**
   * Get all labels from all nodes.
   * @param connection db connection
   * @return SQL result set
   * @throws SQLException
   */
  private ResultSet getProperties(Connection connection) throws SQLException {
    String sql = "SELECT id, attName, attValue, ontID FROM concept_attributes" +
        " WHERE attName NOT IN ('label');";
    PreparedStatement s = connection.prepareStatement(sql);

    return s.executeQuery();
  }


  /**
   * Get all labels from all nodes.
   * @param connection db connection
   * @return SQL result set
   * @throws SQLException
   */
  private ResultSet getLabels(Connection connection) throws SQLException {
    String property = "label";
    String sql = "SELECT id, attValue FROM concept_attributes" +
      " WHERE attName = ?";
    PreparedStatement s = connection.prepareStatement(sql);
    s.setString(1, property);

    return s.executeQuery();
  }

  /**
   * Get all nodes from a given connection.
   * @param con db connection
   * @return SQL result set
   * @throws SQLException
   */
  private ResultSet getNodes(Connection con) throws SQLException {
    String sql = "SELECT c.id, c.url, c.ontID_fk, cc.ccID FROM concept AS c," +
      "connectedComponents AS cc " +
      "WHERE c.id = cc.conceptID ORDER BY cc.ccID;";
    PreparedStatement s = con.prepareStatement(sql);

    return s.executeQuery();
  }

  /**
   * Simple helper class to express a pair of ids building an edge.
   */
  public class Pair {
    Integer source;
    Integer target;

    public Integer getTarget() {
      return target;
    }

    public Integer getSource() {
      return source;
    }

    Pair(Integer p1, Integer p2) {
      this.source = p1;
      this.target = p2;
    }
  }
}
