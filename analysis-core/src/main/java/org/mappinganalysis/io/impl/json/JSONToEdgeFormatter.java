package org.mappinganalysis.io.impl.json;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONObject;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

public class JSONToEdgeFormatter
    extends JSONToEntity
    implements MapFunction<String, Edge<Long, ObjectMap>> {

  private static final Logger LOG = Logger.getLogger(JSONToVertexFormatter.class);

  @Override
  public Edge<Long, ObjectMap> map(String value) throws Exception {
    JSONObject jsonEdge = new JSONObject(value);

    Long source = jsonEdge.getLong(Constants.SOURCE);
    Long target = jsonEdge.getLong(Constants.TARGET);
    LOG.info("#####source: " + source + " target: " + target);
    ObjectMap properties = getProperties(jsonEdge);
    LOG.info("#####properties: " + properties.toString());

    return new Edge<>(source, target, properties);
  }
}
