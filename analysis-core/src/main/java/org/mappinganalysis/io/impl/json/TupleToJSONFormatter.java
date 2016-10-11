package org.mappinganalysis.io.impl.json;

import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * Write tuple datasets to JSON text files.
 * @param <V> TupleX
 */
public class TupleToJSONFormatter<V extends Tuple>
    implements TextOutputFormat.TextFormatter<V> {
  @Override
  public String format(V tuple) {
    JSONObject json = new JSONObject();

    for (int i = 0; i < 20; i++) {
      try {
        json.put("t".concat(Integer.toString(i)), tuple.getField(i).toString());
      } catch (JSONException e) {
        e.printStackTrace();
      } catch (IndexOutOfBoundsException iob) {
        break;
      }
    }

    return json.toString();
  }
}
