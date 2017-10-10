package org.mappinganalysis.model.functions.incremental;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.model.impl.Representative;
import org.mappinganalysis.model.impl.RepresentativeMap;

public class VertexRepresentativeMapFunction
    implements MapFunction<Vertex<Long,ObjectMap>, Representative> {
  private static final Logger LOG = Logger.getLogger(VertexRepresentativeMapFunction.class);

  private DataDomain domain;
  private BlockingStrategy blockingStrategy;

  public VertexRepresentativeMapFunction(DataDomain domain, BlockingStrategy blockingStrategy) {
    this.domain = domain;
    this.blockingStrategy = blockingStrategy;
  }

  @Override
  public Representative map(Vertex<Long, ObjectMap> value) throws Exception {
    LOG.info("####");
    Representative representative = new Representative(value, domain);
    LOG.info("rep: " + representative.toString());

    RepresentativeMap props = representative.getValue();
    LOG.info(props.size());
//    props.setBlockingKey(blockingStrategy);
    LOG.info(props.size());
    LOG.info("props: " + props);
//    representative.setBlockingKey(blockingStrategy);

    return representative;
  }
}
