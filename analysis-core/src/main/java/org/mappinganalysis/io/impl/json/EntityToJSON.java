package org.mappinganalysis.io.impl.json;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.mappinganalysis.model.ObjectMap;

import java.util.Set;

public class EntityToJSON {
  private static final Logger LOG = Logger.getLogger(EntityToJSON.class);

  /**
   * Writes key value pairs from object map to json object.
   * @param map key value object map
   * @return json object
   * @throws JSONException
   */
  protected JSONObject writeProperties(ObjectMap map) throws JSONException {
    JSONObject data = new JSONObject();
    if (map.size() > 0) {
      for (String key : map.keySet()) {
        LOG.info(key + " value: " + map.get(key));
        if (map.get(key) instanceof Set) {
          data.put(key, writeJSONArray((Set) map.get(key)));
        } else {
          data.put(key, map.get(key));
        }
      }
    }
    return data;
  }

  /**
   * Writes property values from a set contained in object map to json object.
   * @param propertySet property values
   * @return json object
   * @throws JSONException
   */
  protected JSONArray writeJSONArray(Set propertySet) throws JSONException {
    JSONArray jsonArray = new JSONArray();
    if (propertySet.size() > 0) {
      for (Object property : propertySet) {
        jsonArray.put(property);
      }
    }
    return jsonArray;
  }
}
