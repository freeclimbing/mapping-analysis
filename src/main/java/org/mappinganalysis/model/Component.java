package org.mappinganalysis.model;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.Syntax;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Resource;
import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import org.apache.jena.atlas.web.HttpException;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.mappinganalysis.utils.Utils;

import java.util.HashSet;
import java.util.Set;

/**
 * Component
 */
public class Component {

  /**
   * Logger
   */
  private static final Logger LOG = Logger.getLogger(Component.class);
  /**
   *  Vertices of component
   */
  private Set<Vertex> vertices = new HashSet<>();
  /**
   * MongoDB
   */
  private MongoDatabase db = null;
  /**
   * Constructor
   * @param db db
   * @param id component id
   */
  public Component(MongoDatabase db, Long id) {
    this.db = db;
    populateVertices(id);
    addVertexURLs();
    addLabels();
    addEdges();
  }

  /**
   * Add all edges to all vertices. Incoming and outgoing edges are not
   * distinguished.
   */
  private void addEdges() {
    MongoCollection<Document> edgeCol = db.getCollection(Utils.COL_EDGES);

    for (Vertex vertex : vertices) {
      BasicDBObject subjectQuery =
        new BasicDBObject(Utils.EDGES_ATTR_SUBJECT, vertex.getId());
      BasicDBObject objectQuery =
        new BasicDBObject(Utils.EDGES_ATTR_OBJECT, vertex.getId());

      for (Document doc : edgeCol.find(subjectQuery)) {
        int edge = doc.get(Utils.EDGES_ATTR_OBJECT, Integer.class);
        vertex.addEdge(edge);
        LOG.info("Added edge: " + edge + " to vertex: " + vertex.getId());
      }
      for (Document doc : edgeCol.find(objectQuery)) {
        int edge = doc.get(Utils.EDGES_ATTR_SUBJECT, Integer.class);
        vertex.addEdge(edge);
        LOG.info("Added edge: " + edge + " to vertex: " + vertex.getId());
      }
    }
  }

  /**
   * Add labels to instances. Labels are retrieved via Virtuoso. Label can
   * be empty.
   */
  private void addLabels() throws org.apache.jena.atlas.web.HttpException {
    String endpoint = "http://linklion.org:8890/sparql";
    String geoName = "http://www.geonames.org/ontology#name";
    String skosLabel = "http://www.w3.org/2004/02/skos/core#prefLabel";
    String rdfsLabel = "http://www.w3.org/2000/01/rdf-schema#label";
    MongoCollection<Document> labelCol = db.getCollection(Utils.COL_LABELS);

    // create label index on first run
    //labelCol.createIndex(new BasicDBObject(Utils.LABELS_ATTR_ID, 1)
    //  .append("unique", true));

    for (Vertex vertex : vertices) {
      String url = vertex.getUrl();
      if (url.startsWith("http://dbpedia.org")
        && !db.getName().equals("hartung")) { // labels already existing
        endpoint = "http://dbpedia.org/sparql";
      }

      BasicDBObject qObject = new BasicDBObject(Utils.LABELS_ATTR_ID,
        vertex.getId());
      MongoCursor<Document> cursor = labelCol.find(qObject, Document.class)
        .iterator();
      if (cursor.hasNext()) {
        System.out.println("############## result");
        Document labelDoc = cursor.next();
      }

//      vertex.setUrl(urlDoc.get(Utils.VERTICES_ATTR_RES, String.class));
//      String query = "SELECT * FROM <http://www.linklion.org/geo-properties> " +
      try {
        String query = "SELECT * " +
          "WHERE { <" + url + "> ?p ?o } ORDER BY ?p ?o";
        Query jenaQuery = QueryFactory.create(query, Syntax.syntaxARQ);
        QueryExecution qExec = QueryExecutionFactory.sparqlService(endpoint, jenaQuery);
        endpoint = "http://linklion.org:8890/sparql";
        ResultSet result = qExec.execSelect();

        while (result.hasNext()) {
          QuerySolution line = result.next();
          try {
            Literal object = line.getLiteral("o");
            Resource predicate = line.getResource("p");
            String uri = predicate.getURI();
            if (uri.equals(geoName) || uri.equals(skosLabel) || uri.equals
              (rdfsLabel)) {
              String label = object.getString();
              vertex.setLabel(label);
              updateMongoDbLabel(labelCol, vertex.getId(), label);
              if (object.getLanguage().equals("en") ||   // best option
                object.getLanguage().equals("")) {      // second best option
                LOG.info("english language detected || language empty");
                break;
              }
              LOG.info("predicate: " + predicate.getURI());

              LOG.info("object ###language: " + object.getLanguage() +
                " ###string: " + label);
            }
          } catch (ClassCastException ignored) {
          }
        }
      } catch (HttpException ignored) {
      }
    }
  }

  /**
   * Add label to MongoDB - labels are rewritten in each run, even if they
   * are already there. Could be optimized.
   */
  private void updateMongoDbLabel(MongoCollection<Document> labelCol, Integer vId, String label) {
    Document data = new Document(Utils.LABELS_ATTR_ID, vId)
      .append(Utils.LABELS_ATTR_LABEL, label);
    LOG.info("## Write " + label + " to vertex " + vId);
    labelCol.replaceOne(new BasicDBObject(Utils.LABELS_ATTR_ID, vId),
      data, (new UpdateOptions()).upsert(true));
  }

  /**
   * Add the URLs to the vertices. URL is mandatory.
   */
  private void addVertexURLs() {
    MongoCollection<Document> urlCol = db.getCollection(Utils.COL_VERTICES);
    for (Vertex vertex : vertices) {
      Document urlDoc = urlCol
        .find(new BasicDBObject(Utils.VERTICES_ATTR_COUNT, vertex.getId()))
        .iterator().next();
      vertex.setUrl(urlDoc.get(Utils.VERTICES_ATTR_RES, String.class));
      LOG.info("url set to " + urlDoc);
    }
  }

  /**
   * Create vertex object for the component ID.
   * @param compId unique component id
   */
  private void populateVertices(Long compId) {
    BasicDBObject idQuery = new BasicDBObject(Utils.CC_ATTR_COMPONENT, compId);
    MongoCollection<Document> col = db.getCollection(Utils.COL_CC);

    for (Document doc : col.find(idQuery)) {
      int vertexId = doc.get(Utils.CC_ATTR_VERTEX, Integer.class);
      LOG.info("vertex id: " + vertexId);
      vertices.add(new Vertex(vertexId));
    }
  }

  /**
   * Get set of vertices for a single component.
   * @return set of vertices
   */
  public Set<Vertex> getVertices() {
    return vertices;
  }

  public Set<Integer> getVerticesAsInt() {
    Set<Integer> vertices = new HashSet<>();

    for (Vertex vertex : getVertices()) {
      vertices.add(vertex.getId());
    }

    return vertices;
  }
}
