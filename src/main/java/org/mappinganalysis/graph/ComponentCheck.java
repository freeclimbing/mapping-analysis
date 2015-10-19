package org.mappinganalysis.graph;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.Component;
import org.mappinganalysis.model.Vertex;
import org.mappinganalysis.utils.DbOps;
import org.mappinganalysis.utils.HaversineGeoDistance;
import org.mappinganalysis.utils.Utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Check components for 1:n links, dissolve into single components.
 */
public class ComponentCheck {
  private static final Logger LOG = Logger.getLogger(ComponentCheck.class);

  private static final String STRATEGY_EXCLUDE = "strategy-exclude";

  private HashSet<Component> components;
  private HashMap<Integer, String> labels;
  private HashSet<Pair> edges;
  private HashSet<Vertex> vertices;
  private int edgeCount;
  private int verticesCount;

  String strategy = "";
  DbOps dbOps = null;

  public ComponentCheck(String strategy, String dbName) throws SQLException {
    this.strategy = strategy;
    this.dbOps = new DbOps(dbName);
  }

  public static void main(String[] args) throws Exception {
    BasicConfigurator.configure();

    ComponentCheck check = new ComponentCheck(STRATEGY_EXCLUDE, Utils.GEO_PERFECT_DB_NAME);

    boolean doCcComputation = true;
    check.preprocessing(check.dbOps.getCon(), !doCcComputation);

    int count = 0;
    for (Component component : check.components) {
      if (count > 1) {
        break;
      }
      LOG.info("Component: " + component.getId());
      if (check.simpleCompare(component.getVertices())) {
        ++count;
      }
    }
    System.out.println("complete components: " + count);
//
//    printStats(check);

//    check.process();

//    worker.printComponents();
  }

  /**
   * Populate vertices and edges for further analysis. Compute connected components at the end.
   * @param connection db connection
   * @param doCcComputation if true, compute connected components new for this run. if false, use db data.
   * @throws Exception
   */
  private void preprocessing(Connection connection, boolean doCcComputation) throws Exception {
    ResultSet resLabels = getLabels(connection);
    setLabels(resLabels);

    readDbVertices(connection);
    HashSet<Integer> flinkVertices = createFlinkVertices(connection);
    HashSet<Tuple2<Integer, Integer>> flinkEdges = createDbAndFlinkEdges(connection);

    if (doCcComputation) {
      long startTime = System.nanoTime();
      int maxIterations = 1000;
      LOG.info("Compute Flink connected components ...");
      FlinkConnectedComponents connectedComponents = new FlinkConnectedComponents();
      DataSet<Tuple2<Integer, Integer>> flinkResult = connectedComponents.compute(flinkVertices, flinkEdges, maxIterations);
      long endTime = System.nanoTime();
      long duration = (endTime - startTime);  //divide by 1000000 to get milliseconds.#
      long distinctComps = flinkResult.project(1).distinct().count();

      // set CC in db
      List<Tuple2<Integer, Integer>> vertexComponentList = flinkResult.collect();
      for (Tuple2<Integer, Integer> vertexAndCc : vertexComponentList) {
        // TODO fix property value to string
        dbOps.updateDbProperty(Utils.DB_CONCEPTID_FIELD, vertexAndCc.f0,
            Utils.DB_CC_TABLE, Utils.DB_CCID_FIELD, String.valueOf(vertexAndCc.f1));
      }
      LOG.info("Computed " + distinctComps + " connected components in " + duration / 1000000 + " ms with Flink.");
    }

    ResultSet resVertices = getVerticesWithPrecomputedCcId(connection);
    addVerticesToComponents(resVertices);

    ResultSet properties = getProperties(connection);
    addProperties(properties);

    LOG.info("Created " + getVerticesCount() + " vertices.");
    LOG.info("With strategy '" + strategy + "' " + (getEdgeCount() - edges.size()) + " edges have been removed.");
    LOG.info("Created " + edges.size() + " edges.");
  }

  /**
   * Create all edges for Flink and for further processing. Strategy for potential edge removal is executed here, too.
   * @param connection db connection
   * @return edge set ready for Flink processing
   * @throws SQLException
   */
  public  HashSet<Tuple2<Integer, Integer>> createDbAndFlinkEdges(Connection connection) throws SQLException {
    LOG.info("Create edges ...");
    edges = Sets.newHashSet();
    ResultSet resEdges = getEdges(connection);
    while (resEdges.next()) {
      edges.add(new Pair(resEdges.getInt(1), resEdges.getInt(2)));
    }
    setEdgeCount(edges.size());

    if (strategy.equals(STRATEGY_EXCLUDE)) {
      ResultSet excludeResultSet = retrieveOneToManyLinksWithOntology(connection);
      removeProblemEdges(excludeResultSet);
    }

    HashSet<Tuple2<Integer, Integer>> flinkEdges = new HashSet<>();
    for (Pair edge : edges) {
      flinkEdges.add(new Tuple2<>(edge.getSrcId(), edge.getTrgId()));
      // not working, vertex needs to be changed within component
//      Vertex vertex = getVertex(from);
//      vertices.remove(vertex);
//      vertex.addEdge(to);
//      vertices.add(vertex);
    }
    return flinkEdges;
  }

  /**
   * Read vertices from db connection and prepare the simple set of vertex ids for Flink computation.
   * @param connection db connection
   * @return set of vertex ids
   * @throws SQLException
   */
  public HashSet<Integer> createFlinkVertices(Connection connection) throws SQLException {
    LOG.info("Create Flink vertices ...");
    ResultSet resVertices = getVertices(connection);
    HashSet<Integer> flinkVertices = Sets.newHashSet();
    while (resVertices.next()) {
      flinkVertices.add(resVertices.getInt(Utils.DB_ID_FIELD));
    }
    return flinkVertices;
  }

  /**
   * Read vertices from db connection and create vertices for further processing.
   * @param connection db connection
   * @throws SQLException
   */
  public void readDbVertices(Connection connection) throws SQLException {
    LOG.info("Read vertices from database for application ...");
    vertices = Sets.newHashSet();
    ResultSet resVertices = getVertices(connection);
    while (resVertices.next()) {
      int id = resVertices.getInt(Utils.DB_ID_FIELD);
      String url = resVertices.getString(Utils.DB_URL_FIELD);
      String ontology = resVertices.getString(Utils.DB_ONTID_FIELD);
      vertices.add(new Vertex(id, url, ontology, labels.get(id)));
    }
    setVerticesCount(vertices.size());
  }

  /**
   * Remove all edges where vertices have 1:n edges for ontologies.
   * @param excludeResultSet SQL result set
   * @throws SQLException
   */
  private void removeProblemEdges(ResultSet excludeResultSet) throws SQLException {
    while (excludeResultSet.next()) {
      int sourceId = excludeResultSet.getInt(1);
      String problemOntology = excludeResultSet.getString(2);
//      HashSet<Integer> vertexEdgeSet = getVertex(sourceId).getEdges(); // TODO check if needed for linklion dataset
//      if (vertexEdgeSet != null || !vertexEdgeSet.isEmpty()) { // "if" not tested/needed in small dataset
//        for (Integer targetId : vertexEdgeSet) {
//          String source = getVertex(targetId).getOntology();
//          System.out.println("t: " + source);
//          if (source.equals(problemOntology)) {
//            System.out.println("exclude targetId to " + targetId);
//            edges.remove(new Pair(targetId, sourceId));
//            edges.remove(new Pair(sourceId, targetId));
//          }
//        }
//      } else { //rly? (working for small data set)
        HashSet<Pair> excludeSet = Sets.newHashSet();
        for (Pair edge : edges) {
          if (edge.getSrcId().equals(sourceId)) {
            if (getVertex(edge.getSrcId()).getOntology().equals(problemOntology)) {
              excludeSet.add(edge);
            }
          } else if (edge.getTrgId().equals(sourceId)) {
            if (getVertex(edge.getTrgId()).getOntology().equals(problemOntology)) {
              excludeSet.add(edge);
            }
          }
        }
        for (Pair pair : excludeSet) {
          edges.remove(pair);
        }
//      }
    }
  }

  /**
   * Get single vertex by id.
   * @param id vertex id
   * @return vertex
   */
  public Vertex getVertex(int id) {
    for (Vertex vertex : vertices) {
      if (vertex.getId() == id) {
        return vertex;
      }
    }
    return null;
  }

  /**
   * Print statistics from the dataset.
   * @param check process
   */
  private static void printStats(ComponentCheck check) {
    int nytCount = 0;
    int missingTypeCount = 0;
    int missingLatLonCount = 0;
    int missingBothCount = 0;
    for (Component component : check.components) {
      for (Vertex vertex : component.getVertices()) {
        boolean isNyt = Boolean.FALSE;
        if (vertex.getOntology().equals("http://data.nytimes.com/")) {
          ++nytCount;
          isNyt = Boolean.TRUE;
        }
        boolean bothMissingPremise = Boolean.FALSE;
        if (!isNyt && vertex.getTypeSet().isEmpty()) {
          ++missingTypeCount;
          bothMissingPremise = Boolean.TRUE;
        }
        if (vertex.getLat() == 0 || vertex.getLon() == 0) {
          ++missingLatLonCount;
          if (bothMissingPremise) {
            ++missingBothCount;
          }
        }
      }
    }
    LOG.info("#########################");
    LOG.info("NYT resources: " + nytCount);
    LOG.info("Missing type (nyt resources are excluded here, no type available): " + missingTypeCount);
    LOG.info("##########################");
    LOG.info("Missing lat/lon: " + missingLatLonCount);
    LOG.info("Missing both: " + missingBothCount);
  }

  private boolean simpleCompare(HashSet<Vertex> compVertices) {
    double lat = 0;
    double lon = 0;
    for (Vertex vertex : compVertices) {
      if (vertex.getLat() == 0.0 || vertex.getLon() == 0.0) {
        return false;
      } else if (!vertex.getOntology().equals("http://data.nytimes.com/") && vertex.getTypeSet().isEmpty()) {
        return false;
      }
    }
    for (Vertex vertex : compVertices) {
//      if (lat != 0) {
//        double result = HaversineGeoDistance.distance(lat, lon, vertex.getLat(), vertex.getLon());
//        System.out.println("##### distance to last vertex: " + result/1000 + " km");
//      }
      LOG.info(vertex.toString());
      HashSet<Integer> neighbors = getNeighbors(vertex.getId());
      LOG.info("edges to: " + neighbors.toString());

      lat = vertex.getLat();
      lon = vertex.getLon();
    }
    return true;
  }

  /**
   * Loop through all components to check quality of contained vertices.
   */
  private void process() {
    for (Component component : components) {
      HashSet<String> uniqueOntologies = new HashSet<>();
      System.out.println("Component: " + component.getId());
      traverse(component, uniqueOntologies, component.getVertices().iterator().next(), Sets.<Vertex>newLinkedHashSet());

//      for (Vertex vertex : component.getVertices()) {
//        LinkedHashSet<Vertex> processed = Sets.newLinkedHashSet();
//        System.out.println("######## next vertex: " + vertex.getId());
//        traverse(component, uniqueOntologies, vertex, processed);
//      }
    }
  }

  /**
   * Only used for testing.
   * @return set components
   */
  public HashSet<Component> getComponentsWithOneToManyInstances() {
    HashSet<Component> excludedComponents = new HashSet<>();

    for (Component component : components) {
      HashSet<String> uniqueOntologies = new HashSet<>();
      for (Vertex vertex : component.getVertices()) {
        if (!uniqueOntologies.add(vertex.getOntology())) {
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
    HashSet<Integer> neighbors = getNeighbors(vertex.getId());
    System.out.println("--- next vertex: " + vertex.getId() + " " + vertex.getUrl());

    if (!uniqueOntologies.add(vertex.getOntology())) {
      LOG.info(vertex.getId() + ": already processed a vertex with source: " + vertex.getOntology() );
      return;
      // 1. case: error found
      // * check path backwards
      // * check neighbors
      // * create new component

//      Vertex dupOntVertex = findDuplicateOntologyVertex(vertex, processed);
//      Vertex sourceVertex = processed.iterator().next();
    } else {
      System.out.println(vertex.getId() + ": vertex not yet processed, process... ");
      if (!neighbors.isEmpty()) {
        System.out.println(vertex.getId() + ": neighbors: " + neighbors);
        for (Integer neighbor : neighbors) {
          System.out.println(vertex.getId() + ": -- is neighbor " + neighbor + " already contained in processed vertices?");
          Vertex neighborVertex = component.getVertex(neighbor);
          System.out.println(neighborVertex.getUrl());
          if (processed.contains(neighborVertex)) {
            System.out.println(vertex.getId() + ": yes, neighbor already existing " + neighbor);
            // 3. case: vertex already processed
//            break;
          } else {
            // 4. process next vertex
            // Vertex next = component.getVertex(neighbor);//neighbors.iterator().next());
            processed.add(vertex);
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
   *
   * @param vertices vertices
   * @param resEdges result set
   * @throws SQLException
   */
  private int addEdges(HashSet<Vertex> vertices, ResultSet resEdges) throws SQLException {
    while (resEdges.next()) {
      int source = resEdges.getInt(1);
      int target = resEdges.getInt(2);

      for (Vertex vertex : vertices) {
        if (vertex.getId() == source) {
          vertex.addEdge(target);
        }
      }
      Pair pair = new Pair(source, target);
      edges.add(pair);
    }
    return edges.size();
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
      if (checkVertex.getOntology().equals(vertex.getOntology())) {
        System.out.println(checkVertex.getId() + " same source like " + vertex.getId());
        return checkVertex;
      }
    }
    return null;
  }

  /**
   * Get neighbors for a given vertex
   * @param id vertex id
   * @return set of vertex ids for neighbors
   */
  private HashSet<Integer> getNeighbors(int id) {
    HashSet<Integer> neighbors = new HashSet<>();
    for (Pair edge : edges) {
      if (edge.getSrcId() == id) {
        neighbors.add(edge.getTrgId());
      } else if (edge.getTrgId() == id) {
        neighbors.add(edge.getSrcId());
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
                vertex.setLon(Double.parseDouble(value));
              break;
            case "type":
              vertex.addType(value);
              break;
            case "ele":
              vertex.setEle(Double.parseDouble(value));
              break;
            case "typeDetail":
              vertex.setTypeDetail(value);
              break;
            default:
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
  private void addVerticesToComponents(ResultSet resNodes) throws SQLException {
    LOG.info("Add vertices to application components ...");
    components = Sets.newHashSet();
    while (resNodes.next()) {
      int id = resNodes.getInt(1);
      String url = resNodes.getString(2);
      String ontology = resNodes.getString(3);
      int ccId = resNodes.getInt(4);

      Vertex vertex = new Vertex(id, url, ontology, labels.get(id));
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
      Set<Integer> verticesIntSet = component.getVerticesAsInt();
      Set<Vertex> v = component.getVertices();
      String componentLabels = "";
      for (Vertex vertex : v) {
        componentLabels += vertex.getLabel() + " ";
      }
      LOG.info(component.getId() + ": [" + verticesIntSet.toString() + "]");
      LOG.info(componentLabels);
    }
  }

  /**
   * Populate labels from SQL result set.
   * @param resLabels SQL result set
   * @throws SQLException
   */
  public void setLabels(ResultSet resLabels) throws SQLException {
    LOG.info("Set labels in application...");
    labels = Maps.newHashMap();
    while (resLabels.next()) {
      labels.put(resLabels.getInt(1), resLabels.getString(2));
    }
  }


  private ResultSet retrieveOneToManyLinksWithOntology(Connection connection) throws SQLException {
    String sql = "SELECT " +
        "    a.trgID as resultID, targetOnt as ontology " +
        "FROM " +
        "    (SELECT srcID, trgID, ontID_fk AS sourceOnt, url " +
        "    FROM linksWithIDs, concept " +
        "    WHERE srcID = id) AS a " +
        "        LEFT OUTER JOIN " +
        "    (SELECT srcID, trgID, ontID_fk AS targetOnt, url " +
        "    FROM linksWithIDs, concept " +
        "    WHERE trgID = id) AS b " +
        "    ON a.srcID = b.srcID AND a.trgID = b.trgID " +
        "GROUP BY a.trgID , sourceOnt , targetOnt " +
        "HAVING COUNT(a.trgID) > 1 " +
        "UNION ALL " +
        "SELECT " +
        "    a.srcID as resultID, sourceOnt as ontology " +
        "FROM " +
        "    (SELECT srcID, trgID, ontID_fk AS sourceOnt, url " +
        "    FROM linksWithIDs, concept " +
        "    WHERE srcID = id) AS a " +
        "        LEFT OUTER JOIN " +
        "    (SELECT srcID, trgID, ontID_fk AS targetOnt, url " +
        "    FROM linksWithIDs, concept " +
        "    WHERE trgID = id) AS b  " +
        "    ON a.srcID = b.srcID AND a.trgID = b.trgID " +
        "GROUP BY a.srcID , sourceOnt , targetOnt " +
        "HAVING COUNT(a.srcID) > 1;";
    PreparedStatement s = connection.prepareStatement(sql);

    return s.executeQuery();
  }

  /**
   * Get all properties from all vertices. (ontID is empty in attributes)
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
  public ResultSet getLabels(Connection connection) throws SQLException {
    LOG.info("Get labels from database...");
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
  private ResultSet getVertices(Connection con) throws SQLException {
    String sql = "SELECT id, url, ontID_fk FROM concept;";
    PreparedStatement s = con.prepareStatement(sql);

    return s.executeQuery();
  }

  /**
   * Get all nodes with ccid from a given connection.
   * @param con db connection
   * @return SQL result set
   * @throws SQLException
   */
  private ResultSet getVerticesWithPrecomputedCcId(Connection con) throws SQLException {
    LOG.info("Get additional properties from database ...");
    String sql = "SELECT c.id, c.url, c.ontID_fk, cc.ccID FROM concept AS c," +
      "connectedComponents AS cc " +
      "WHERE c.id = cc.conceptID ORDER BY cc.ccID;";
    PreparedStatement s = con.prepareStatement(sql);

    return s.executeQuery();
  }

  public int getEdgeCount() {
    return edgeCount;
  }

  public void setEdgeCount(int edgeCount) {
    this.edgeCount = edgeCount;
  }

  public int getVerticesCount() {
    return verticesCount;
  }

  public void setVerticesCount(int verticesCount) {
    this.verticesCount = verticesCount;
  }


  public HashSet<Pair> getEdges() {
    return edges;
  }

  public void setEdges(HashSet<Pair> edges) {
    this.edges = edges;
  }

  public HashSet<Vertex> getVertices() {
    return vertices;
  }

  public void setVertices(HashSet<Vertex> vertices) {
    this.vertices = vertices;
  }

  public HashSet<Component> getComponents() {
    return components;
  }

  /**
   * Simple helper class to express a pair of ids building an edge.
   */
  public class Pair {
    Integer source;
    Integer target;

    public Integer getTrgId() {
      return target;
    }

    public Integer getSrcId() {
      return source;
    }

    Pair(Integer p1, Integer p2) {
      this.source = p1;
      this.target = p2;
    }
  }
}
