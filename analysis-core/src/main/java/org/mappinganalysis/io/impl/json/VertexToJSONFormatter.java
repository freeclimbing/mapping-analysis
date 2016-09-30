package org.mappinganalysis.io.impl.json;

import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.graph.Vertex;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

/**
 * Vertex to JSON Formatter
 */
public class VertexToJSONFormatter<V extends Vertex<Long, ?>>
    extends EntityToJSON
    implements TextOutputFormat.TextFormatter<V> {
  @Override
  public String format(V v) {
    JSONObject json = new JSONObject();
    try {
      json.put(Constants.ID, v.getId());
      if (v.getValue() instanceof ObjectMap) {
        json.put(Constants.DATA, writeProperties((ObjectMap) v.getValue()));
      } else if (v.getValue() instanceof Long) {
        json.put(Constants.DATA, v.getValue());
      }
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return json.toString();
  }
}
