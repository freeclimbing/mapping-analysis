package org.mappinganalysis.io.impl.json;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONObject;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

public class JSONToVertexFormatter
    extends JSONToEntity
    implements MapFunction<String, Vertex<Long, ObjectMap>> {

  private static final Logger LOG = Logger.getLogger(JSONToVertexFormatter.class);

  @Override
  public Vertex<Long, ObjectMap> map(String value) throws Exception {
    JSONObject jsonVertex = new JSONObject(value);

    Long id = jsonVertex.getLong(Constants.ID);
//    LOG.info("vertex id: " + id);
    ObjectMap properties = getProperties(jsonVertex);
//    LOG.info("vertex properties: " + properties.toString());

    return new Vertex<>(id, properties);
  }
}
