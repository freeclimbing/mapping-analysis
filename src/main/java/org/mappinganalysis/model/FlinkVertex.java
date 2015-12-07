package org.mappinganalysis.model;

import org.apache.flink.graph.Vertex;
import org.mappinganalysis.utils.Utils;

import java.util.Map;

/**
 * Flink vertex.
 *
 * f0: vertex identifier
 * f1: vertex properties
 */
public class FlinkVertex extends Vertex<Long, Map<String, Object>> {

  public FlinkVertex() {
  }

  public FlinkVertex(long vId, Map<String, Object> properties) {
    f0 = vId;
    f1 = properties;
  }

  public Long getId() {
    return f0;
  }

  public void setId(Long vertexId) {
    f0 = vertexId;
  }

  public Map<String, Object> getProperties() {
    return f1;
  }

  public void setProperties(Map<String, Object> properties) {
    f1 = properties;
  }

  public boolean hasGeoCoords() {
     return (f1.containsKey(Utils.LAT) && f1.containsKey(Utils.LON)) ? Boolean.TRUE : Boolean.FALSE;
  }

  public GeoCode getGeoCoords() {
    return new GeoCode((double) f1.get(Utils.LAT), (double) f1.get(Utils.LON));
  }

  public boolean hasLabel() {
    return f1.containsKey(Utils.LABEL) ? Boolean.TRUE : Boolean.FALSE;
  }

  public Object getLabel() {
    return f1.get(Utils.LABEL);
  }
}
