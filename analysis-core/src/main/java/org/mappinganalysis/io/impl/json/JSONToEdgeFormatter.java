package org.mappinganalysis.io.impl.json;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.Edge;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONObject;
import org.mappinganalysis.util.Constants;

public class JSONToEdgeFormatter<EV>
    extends JSONToEntity
    implements MapFunction<String, Edge<Long, EV>>, ResultTypeQueryable {
  private Class edgeClass;

  public JSONToEdgeFormatter(Class<?> edgeClass) {
    this.edgeClass = edgeClass;
  }

  @Override
  public Edge<Long, EV> map(String value) throws Exception {
    JSONObject jsonEdge = new JSONObject(value);

    Long source = jsonEdge.getLong(Constants.SOURCE);
    Long target = jsonEdge.getLong(Constants.TARGET);
    Object properties = getProperties(jsonEdge, edgeClass);

    return new Edge<>(source, target, (EV) properties);
  }

  @Override
  public TypeInformation<Edge<Long, EV>> getProducedType() {
    return new TupleTypeInfo(Edge.class, BasicTypeInfo.LONG_TYPE_INFO,
        BasicTypeInfo.LONG_TYPE_INFO,
        TypeExtractor.getForClass(edgeClass));
  }
}
