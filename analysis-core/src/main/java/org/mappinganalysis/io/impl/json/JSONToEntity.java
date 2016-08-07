package org.mappinganalysis.io.impl.json;

import com.google.common.collect.Maps;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

import java.util.*;

public class JSONToEntity {

  private static final Logger LOG = Logger.getLogger(JSONToVertexFormatter.class);

  /**
   * Possible property names where several values can occur, i.e, typeIntern=Settlement, Country
   */
  List<String> arrayOptions = Arrays.asList(
      Constants.TYPE_INTERN,
      Constants.COMP_TYPE,
      Constants.ONTOLOGIES,
      Constants.CL_VERTICES
  );

  List<String> longOptions = Arrays.asList(
      Constants.HASH_CC,
      Constants.CC_ID,
      Constants.SOURCE,
      Constants.TARGET
  );

  protected ObjectMap getProperties(JSONObject object) throws JSONException {
    HashMap<String, Object> props =
        Maps.newHashMapWithExpectedSize(object.length() * 2);
    object = object.getJSONObject(Constants.DATA);
    Iterator<?> keys = object.keys();
    while (keys.hasNext()) {
      String key = keys.next().toString();
      //    Caused by: java.lang.ClassCastException: java.lang.String cannot be cast to org.codehaus.jettison.json.JSONArray
      //    typeIntern = AdministrativeRegion
      if (arrayOptions.contains(key)) { //&& object.get(key) instanceof JSONArray) {
        Set subProps = getArrayValues(key, (JSONArray) object.get(key));
        props.put(key, subProps);
      } else if (longOptions.contains(key)) {
        Long longValue = object.getLong(key);
        props.put(key, longValue);
      } else {
        Object o = object.get(key);
        props.put(key, o);
      }
    }

    return new ObjectMap(props);
  }

  private Set getArrayValues(String key, JSONArray array) throws JSONException {
    HashSet values = new HashSet<>();

    if (key.equals(Constants.CL_VERTICES)) {
      for (int i = 0; i < array.length(); i++) {
        values.add(array.getLong(i));
      }
    } else {
      for (int i = 0; i < array.length(); i++) {
        values.add(array.getString(i));
      }
    }

    return values;
  }
}
