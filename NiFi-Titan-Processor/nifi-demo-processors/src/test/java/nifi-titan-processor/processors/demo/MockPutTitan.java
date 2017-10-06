import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * MockPutTitan which allows us to inject test data before running the processor
 */
public class MockPutTitan extends PutTitan {

    @Override
    public StandardTitanGraph createTitanGraph(BaseConfiguration baseConfiguration) {

        StandardTitanGraph graph = (StandardTitanGraph) TitanFactory.open(baseConfiguration);

        TitanTransaction tx = graph.newTransaction();

        Vertex yrk = tx.addVertex(T.label, "station", "tiploc", "YRKTL");
        Vertex glc = tx.addVertex(T.label, "station", "tiploc", "GLCTL");

        yrk.addEdge("headcode", glc, "hcode", "XXXX");

        tx.commit();
        tx.close();

        return graph;
    }

    public StandardTitanGraph getGraph() {
        return titanGraph;
    }
}
