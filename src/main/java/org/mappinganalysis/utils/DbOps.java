package org.mappinganalysis.utils;

import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * database operations
 */
public class DbOps {
  private static final Logger LOG = Logger.getLogger(DbOps.class);
  Connection con;
  String dbName;

  public DbOps(String dbName) throws SQLException {
    this.dbName = dbName;
    this.con = Utils.openDbConnection(dbName);
  }

  /**
   * (deprecated) Update a single property value for a single vertex. (only working for col name 'url')
   * @param id vertex id
   * @param key property name
   * @param value new value
   */
  public void updateDbProperty(Integer id, String key, String value) throws SQLException {
    updateDbProperty("id", id, "concept", key, value);
  }

  /**
   * Update single property for a vertex in a table.
   * @param idField field name of id column
   * @param vertexId vertex id
   * @param tableName table name
   * @param fieldName field name property
   * @param value property value
   */
  public void updateDbProperty(String idField, Integer vertexId, String tableName,
                               String fieldName, String value) throws SQLException {
    if (!value.isEmpty() && !fieldName.isEmpty()) {
      String update = "UPDATE ".concat(tableName).concat(" SET ").concat(fieldName)
          .concat(" = ? WHERE ").concat(idField).concat(" = ?;");
      PreparedStatement updStmt = con.prepareStatement(update);
      updStmt.setInt(2, vertexId);
      if (fieldName.equals(Utils.DB_URL_FIELD)
          || fieldName.equals(Utils.DB_ONTID_FIELD)
          || fieldName.equals(Utils.DB_ATT_VALUE_TYPE)) {
        updStmt.setString(1, value);
      } else if (fieldName.equals(Utils.DB_CCID_FIELD)) {
        updStmt.setInt(1, Integer.valueOf(value));
      }
      updStmt.executeUpdate();
      updStmt.close();
//      LOG.info("Written for Vertex: " + vertexId + " Property: " +
//          fieldName + " Value: " + value);
    }
  }

  /**
   * Update single property for a vertex in a table.
   * @param vertexId vertex id
   * @param attName vertex attributes type name
   * @param attValue vertex attributes type value
   * @param value property value
   * @throws SQLException
   */
  public void updateDbAttributesProperty(Integer vertexId, String attName,
                                         String attValue, String value) throws SQLException {
    if (!value.isEmpty()) {
      String update = "UPDATE concept_attributes"
          .concat(" SET attValueType = ? WHERE id = ? AND attName = ? AND attValue = ?;");
      PreparedStatement updStmt = con.prepareStatement(update);
      updStmt.setString(1, value);
      updStmt.setInt(2, vertexId);
      updStmt.setString(3, attName);
      updStmt.setString(4, attValue);
      updStmt.executeUpdate();
      updStmt.close();
    }
  }

  /**
   * Delete single attribute value.
   * @param id vertex id
   * @param value value to delete
   * @throws SQLException
   */
  private void deleteSingleValue(Integer id, String value) throws SQLException {
    if (!value.isEmpty()) {
      String del = "DELETE FROM concept_attributes WHERE attName = 'lon' AND attValue = ? AND id = ?;";

      PreparedStatement stmt = con.prepareStatement(del);
      stmt.setString(1, value);
      stmt.setInt(2, id);

      stmt.executeUpdate();
      stmt.close();
      LOG.info("Deleted for Vertex: " + id + " Value: " + value);
    }
  }

  /**
   * Write single property for single vertex to db. If query is unsuccessful, return key for error processing.
   * @param id vertex id
   * @param key property key
   * @param value property value
   * @return property key, if exception occurs
   * @throws SQLException
   */
  public String writePropertyToDb(Integer id, String key, String value) throws SQLException {
    if (!value.isEmpty() && !key.isEmpty()) {
      String insert = "INSERT INTO concept_attributes (id, attName, " +
          "attValue) VALUES (?, ?, ?);";

      PreparedStatement insertStmt = con.prepareStatement(insert);
      insertStmt.setInt(1, id);
      insertStmt.setString(2, key);
      insertStmt.setString(3, value);
      try {
        insertStmt.executeUpdate();
        System.out.println("Written for Vertex: " + id + " Property: " +
            key + " Value: " + value);
        return key;
      } catch (MySQLIntegrityConstraintViolationException ignore) {
        if (key.equals(Utils.TYPE_NAME)) {
          System.err.println(Utils.TYPE_NAME + "error");
        }
      } finally {
        insertStmt.close();
      }
    }
    return "";
  }

  /**
   * Write potential error while retrieving label to DB for later analysis.
   * @param id node id
   * @param url node url
   * @param e error
   * @param type error type
   * @throws SQLException
   */
  public void writeError(int id, String url, String e, String type) throws
      SQLException {
    String error = "INSERT INTO error_concept (id, url, error, error_type) " +
        "VALUES (?, ?, ?, ?);";

    PreparedStatement insertStmt = con.prepareStatement(error);
    insertStmt.setInt(1, id);
    insertStmt.setString(2, url);
    insertStmt.setString(3, e);
    insertStmt.setString(4, type);
    insertStmt.executeUpdate();
    insertStmt.close();
  }

  /**
   * Write potential error while retrieving label to DB for later analysis.
   * @param id node id
   * @param url node url
   * @param e error
   * @throws SQLException
   */
  public void writeError(int id, String url, String e) throws
      SQLException {
    writeError(id, url, e, "general_error");
  }

  public ResultSet getProperties() throws SQLException {
    String sql = "SELECT id, attName, attValue FROM concept_attributes;";
    PreparedStatement s = con.prepareStatement(sql);

    return s.executeQuery();
  }

  private ResultSet getMaliciousDbpResources() throws SQLException {
    String sql = "SELECT id, url FROM hartung_perfect_geo_links.concept " +
        "where url like '%\\%2C%';";
    PreparedStatement s = con.prepareStatement(sql);

    return s.executeQuery();
  }

  /**
   * Get all nodes without coordinates or type
   * @return SQL result set
   * @throws SQLException
   */
  public ResultSet getAllFreebaseNodes() throws SQLException {
    String sql = "SELECT DISTINCT id, url FROM concept WHERE url like 'http://rdf.freebase.com%';";
    PreparedStatement s = con.prepareStatement(sql);

    return s.executeQuery();
  }

  /**
   * Only needed once per dataset, enrich missing source/ontID_fk fields in db with URLs of vertex.
   * @return SQL result set
   * @throws SQLException
   */
  public ResultSet getNodesMissingSource() throws SQLException {
    String sql = "SELECT DISTINCT id, url FROM concept WHERE ontID_fk IS NULL;";
    PreparedStatement s = con.prepareStatement(sql);

    return s.executeQuery();
  }

  public Connection getCon() throws SQLException {
    return con;
  }
}
