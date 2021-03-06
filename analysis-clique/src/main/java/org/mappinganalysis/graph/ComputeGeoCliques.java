package org.mappinganalysis.graph;

import com.mongodb.client.DistinctIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.log4j.Logger;
import org.bson.BsonValue;
import org.mappinganalysis.model.Component;
import org.mappinganalysis.model.CompCheckVertex;
import org.mappinganalysis.util.Utils;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * Compute cliques for each of the connected components in the input.
 */
public class ComputeGeoCliques {

  private static final Logger LOG = Logger.getLogger(ComputeGeoCliques.class);

  static final String MONGO_DB_HASH = "hash"; //linklion
  static final String HASH_OUT = "geo-linklion.txt";

  /**
   * @param args -
   */
  public static void main(String[] args) throws FileNotFoundException,
      UnsupportedEncodingException {
//    BasicConfigurator.configure();

    MongoDatabase db = Utils.getMongoDatabase(MONGO_DB_HASH);
    MongoCollection<BsonValue> componentIds = db.getCollection("cc", BsonValue.class);
    DistinctIterable<BsonValue> distinctComps = componentIds.distinct
        ("component", BsonValue.class);

    PrintWriter writer = new PrintWriter(HASH_OUT, "UTF-8");

    int processedComponents = 0;
    for (BsonValue compId : distinctComps) {
      long id = compId.asInt32().longValue();

      String message = "processing: " + processedComponents + " # internal component number: " + id;
      writer.println(message);
      System.out.println(message);
    }
    writer.close();

  }

  /**
   * Compute cliques for a single component and write it to disk.
   * @param writer file writer
   * @param component single component
   */
  private static void computeCliques(PrintWriter writer, Component component) {
//    Graph<Integer, DefaultEdge> graph =
//      new DefaultDirectedGraph<>(DefaultEdge.class);
//
//    for (Vertex vertex : component.getVertices()) {
//      graph.addVertex(vertex.getId());
//    }
//    for (Vertex vertex : component.getVertices()) {
//      for (Integer edge : vertex.getEdges()) {
//        graph.addEdge(vertex.getId(), edge);
//      }
//    }
//
//    BronKerboschCliqueFinder<Integer, DefaultEdge> cf =
//      new BronKerboschCliqueFinder<>(graph);
//
//    int cliqueCount = 1;
//    for (Set<Integer> clique : cf.getAllMaximalCliques()) {
//      StringBuilder cliqueString = new StringBuilder("\nClique "+cliqueCount+":\n");
//      for (int c : clique) {
//        String url = "";
//        String label = "";
//        for (Vertex vertex : component.getVertices()) {
//          if (vertex.getId() == c) {
//            url = vertex.getUrl();
//            label = vertex.getLabel();
//          }
//        }
//        cliqueString.append(url+"\t"+label+"\n");
//      }
////        System.out.println(cliqueString);
//      writer.println(cliqueString);
//      cliqueCount++;
//    }

//      // stackoverflow on component 4615 New Jersey
    Set<Integer> nodes = component.getVerticesAsInt();
    HashMap<Integer, List<Integer>> edges = new HashMap<>();
    for (CompCheckVertex vertex : component.getVertices()) {
      List<Integer> list = new ArrayList<>(vertex.getEdges());
      edges.put(vertex.getId(), list);
    }

    CliqueIdentification ci = new CliqueIdentification();
    Set<Set<Integer>> cliqueSet = ci.simpleCluster(nodes, edges);

    int cliqueCount = 1;
    for (Set<Integer> clique : cliqueSet) {
      StringBuilder cliqueString = new StringBuilder("\nClique "+cliqueCount+":\n");
      for (int c : clique) {
        String url = "";
        String label = "";
        for (CompCheckVertex vertex : component.getVertices()) {
          if (vertex.getId() == c) {
            url = vertex.getUrl();
            label = vertex.getLabel();
          }
        }
        cliqueString.append(url+"\t"+label+"\n");
      }
      System.out.println(cliqueString);
      writer.println(cliqueString);
      cliqueCount++;

    }
  }

  private static void getCliques(Long ccId) {

  }
}
