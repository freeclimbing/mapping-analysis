package org.mappinganalysis.model.functions.incremental;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.util.Constants;

public class RepresentativeCreator
    implements CustomUnaryOperation<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(RepresentativeCreator.class);

  private DataSet<Vertex<Long, ObjectMap>> initialVertices;
  private DataDomain domain;
  private BlockingStrategy blockingStrategy;

  public RepresentativeCreator(DataDomain domain, BlockingStrategy blockingStrategy) {
    this.domain = domain;
    this.blockingStrategy = blockingStrategy;
  }

  @Override
  public void setInput(DataSet<Vertex<Long, ObjectMap>> inputData) {
    this.initialVertices = inputData;
  }

  @Override
  public DataSet<Vertex<Long, ObjectMap>> createResult() {
    return initialVertices
        .map(new MapFunction<Vertex<Long,ObjectMap>, Vertex<Long, ObjectMap>>() {
          @Override
          public Vertex<Long, ObjectMap> map(Vertex<Long, ObjectMap> vertex) throws Exception {
            vertex.getValue().setMode(Constants.GEO);
            vertex.getValue().setBlockingKey(BlockingStrategy.STANDARD_BLOCKING);
            LOG.info(vertex.toString());
            return vertex;
          }
        });
//        .map(new VertexRepresentativeMapFunction(domain, blockingStrategy));
  }

}
