package org.mappinganalysis.io.impl.json;

import com.google.common.collect.Maps;
import org.apache.flink.types.NullValue;
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
      Constants.CL_VERTICES,
      Constants.TYPE
  );

  List<String> longOptions = Arrays.asList(
      Constants.HASH_CC,
      Constants.CC_ID,
      Constants.SOURCE,
      Constants.TARGET
  );

  protected <V> V getProperties(JSONObject data, Class<V> entityClass) throws JSONException {

    if (entityClass.equals(ObjectMap.class)) {
      HashMap<String, Object> properties =
          Maps.newHashMapWithExpectedSize(data.length() * 2);
      data = data.getJSONObject(Constants.DATA);

      Iterator<?> keys = data.keys();
      while (keys.hasNext()) {
        String key = keys.next().toString();
        //    Caused by: java.lang.ClassCastException: java.lang.String cannot be cast to org.codehaus.jettison.json.JSONArray
        //    typeIntern = AdministrativeRegion
        if (arrayOptions.contains(key) && !(data.get(key) instanceof String)) { //&& object.get(key) instanceof JSONArray) {
          Set subProps = getArrayValues(key, (JSONArray) data.get(key));
          LOG.info(key + " set value: " + subProps.toString());
          properties.put(key, subProps);
        } else if (longOptions.contains(key)) {
          Long longValue = data.getLong(key);
          LOG.info(key + " long value: " + longValue.toString());
          properties.put(key, longValue);
        } else {
          Object o = data.get(key);
          LOG.info(key + " o value: " + o.toString() + " entity class " + entityClass.toString());
          properties.put(key, o);
        }
      }

      LOG.info(" ####### " + properties.toString());

      return (V) new ObjectMap(properties);
    } else if (entityClass.equals(Long.class)) {
      Long property = data.getLong(Constants.DATA);

      return  (V) property;

    } else if (entityClass.equals(NullValue.class)) {
      return (V) NullValue.getInstance();
    } else {
      return null;
    }
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
