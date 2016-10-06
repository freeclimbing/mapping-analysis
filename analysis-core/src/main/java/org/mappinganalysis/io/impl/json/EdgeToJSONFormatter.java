package org.mappinganalysis.io.impl.json;

import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.graph.Edge;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

/**
 * Edge to JSON Formatter
 */
public class EdgeToJSONFormatter<E extends Edge<Long, ?>>
    extends EntityToJSON
    implements TextOutputFormat.TextFormatter<E> {
  @Override
  public String format(E edge) {
    JSONObject json = new JSONObject();
    try {
      json.put(Constants.SOURCE, edge.getSource());
      json.put(Constants.TARGET, edge.getTarget());
      if (edge.getValue() instanceof ObjectMap) {
        json.put(Constants.DATA, writeProperties((ObjectMap) edge.getValue()));
      }
    } catch (JSONException ex) {
      ex.printStackTrace();
    }
    return json.toString();
  }
}
