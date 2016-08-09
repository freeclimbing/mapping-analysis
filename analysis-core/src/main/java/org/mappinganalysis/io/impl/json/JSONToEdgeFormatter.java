package org.mappinganalysis.io.impl.json;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Table;
import org.apache.flink.hadoop.shaded.org.jboss.netty.handler.codec.serialization.ObjectEncoder;
import org.apache.flink.types.NullValue;
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
//    LOG.info("#####source: " + source + " target: " + target);
    if (jsonEdge.has(Constants.DATA)) {
      return new Edge<>(source, target, getProperties(jsonEdge));
    } else {
      ObjectMap tmp = new ObjectMap();
      tmp.addProperty("foo", "bar"); // TODO dirty
      return new Edge<>(source, target, tmp);
    }
//    LOG.info("#####properties: " + properties.toString());
  }
}
