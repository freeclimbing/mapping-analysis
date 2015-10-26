package org.mappinganalysis.model;

import java.util.HashMap;

/**
 * not yet working
 */
public class PropertyContainer extends HashMap<String, Object> {
  private HashMap<String, Object> properties;
  public PropertyContainer() {
  }

  public HashMap<String, Object> get() {
    return properties;
  }

  public void set(PropertyContainer properties) {
     this.properties = properties;
  }
}
