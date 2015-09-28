package org.mappinganalysis.graph;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.Component;
import org.mappinganalysis.model.Vertex;
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

  HashSet<Component> components = new HashSet<>();
  HashMap<Integer, String> labels = new HashMap<>();
  HashSet<Pair> edges = new HashSet<>();

  public ComponentCheck() {
  }

  public static void main(String[] args) throws SQLException {

    BasicConfigurator.configure();

    Connection connection = Utils.openDbConnection(Utils.GEO_PERFECT_DB_NAME);
    ComponentCheck worker = new ComponentCheck();

    ResultSet resLabels = worker.getLabels(connection);
    worker.setLabels(resLabels);

    ResultSet resNodes = worker.getNodes(connection);
    worker.addNodesToComponents(resNodes);

    ResultSet resEdges = worker.getEdges(connection);
    worker.addEdges(resEdges);


    worker.check();


//    worker.printComponents();
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

  private void check() {
    for (Component component : components) {
      HashSet<String> uniqueOntologies = new HashSet<>();

      for (Vertex vertex : component.getVertices()) {
        LinkedHashSet<Vertex> processed = new LinkedHashSet<>();
        traverse(component, uniqueOntologies, vertex, processed);

      }
    }
  }

  private void traverse(Component component, HashSet<String> uniqueOntologies,
    Vertex vertex, LinkedHashSet<Vertex> processed) {
    Set<Integer> neighbors = getNeighbors(edges, vertex.getId());

    if (!uniqueOntologies.add(vertex.getSource())) {
      // 1. case: error found
      Vertex dupOntVertex = findDuplicateOntologyVertex(vertex, processed);
      Vertex sourceVertex = processed.iterator().next();
    } else {
      if (!neighbors.isEmpty()) { // 2. case: no neighbors
        for (Integer neighbor : neighbors) {
          if (processed.contains(component.getVertex(neighbor))) {
            // 3. case: vertex already processed
            break;
          } else {
            // 4. process next vertex
            Vertex next = component.getVertex(neighbors.iterator().next());
            processed.add(vertex);
            traverse(component, uniqueOntologies, next, processed);
          }
        }
      }
    }
  }

  private Vertex findDuplicateOntologyVertex(Vertex vertex,
    LinkedHashSet<Vertex> processed) {
    for (Vertex checkVertex : processed) {
      if (checkVertex.getSource().equals(vertex.getSource())) {
        return checkVertex;
      }
    }
    return null;
  }

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
   * Add all nodes to components. If component does not exist, create it.
   * @param resNodes SQL result set of all nodes
   */
  private void addNodesToComponents(ResultSet resNodes) throws SQLException {
    while (resNodes.next()) {
      int id = resNodes.getInt(1);
      String url = resNodes.getString(2);
      // not yet working for Bioportal data
      String source = getSource(url);
      int ccId = resNodes.getInt(3);

//      HashSet<Integer> v_edges = new HashSet<>();
//      if (edges.containsKey(id) || edges.containsValue(id)) {
//
//        v_edges.add()
//      }

      Vertex vertex = new Vertex(id, url, source, labels.get(id));
      if (!addToExistingComponent(vertex, ccId)) {
        createComponentWithVertex(vertex, ccId);
      }
    }
  }

  /**
   * Extract source from an URL.
   * @param url instance url
   * @return source string
   */
  private String getSource(String url) {
    String source = "";
    if (url.startsWith("http://")) {
      String regex = "(http:\\/\\/.*?)\\/";
      Pattern pattern = Pattern.compile(regex);
      Matcher matcher = pattern.matcher(url);
      if (matcher.find()) {
        source = matcher.group(1);
      }
    }

    return source;
  }

  /**
   * Create a new component with a single vertex.
   * @param vertex vertex
   * @param ccId component id
   */
  private void createComponentWithVertex(Vertex vertex, int ccId) {
    Component newComponent = new Component(ccId);
    newComponent.addVertex(vertex);
    components.add(newComponent);
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
    String sql = "SELECT c.id, c.url, cc.ccID FROM concept AS c," +
      "connectedComponents AS cc " +
      "WHERE c.id = cc.conceptID ORDER BY cc.ccID;";
    PreparedStatement s = con.prepareStatement(sql);

    return s.executeQuery();
  }


  class Pair {
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
