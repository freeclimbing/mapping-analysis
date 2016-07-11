package org.mappinganalysis.io.debug;

import org.apache.flink.graph.Vertex;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;

public class PrintVertices extends Printer<Vertex<Long, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(PrintVertices.class);

  /**
   * Constructor
   *
   * @param isIterative true, if called in iterative context
   * @param prefix prefix for output
   */
  public PrintVertices(boolean isIterative, String prefix) {
    super(isIterative, prefix);
  }

  @Override
  protected String getDebugString(Vertex<Long, ObjectMap> vertex) {
    return String.format("(%s,%s)",
        vertex.getId(),
        vertex.getValue().toString());
  }

  @Override
  protected Logger getLogger() {
    LOG.setLevel(Level.DEBUG);
    return LOG;
  }
}
