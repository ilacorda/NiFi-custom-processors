import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"TitanDb", "insert", "put", "turbine"})
@CapabilityDescription("Serialise a JSON file and add vertices and one edge to be imported into Titan. " +
        "This is currently a simple example of writing a number of vertices and edges into Titan")
public class PutTitan extends AbstractProcessor {

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Success relationship")
            .build();
    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile that cannot be sent to Titan will be routed to this Relationship")
            .build();

    public static final PropertyDescriptor STORAGE_BACKEND = new PropertyDescriptor.Builder()
            .name("Storage Backend")
            .description("Data Storage Backend Required for Titan")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor STORAGE_HOSTNAME = new PropertyDescriptor.Builder()
            .name("Storage Backend HostName")
            .description("Hostname of the storage backend")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor INDEX_SEARCH_BACKEND = new PropertyDescriptor.Builder()
            .name("Index Search Backend")
            .description("Index Search Backend")
            //set to false for the time being
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor INDEX_SEARCH_HOSTNAME = new PropertyDescriptor.Builder()
            .name("Index Search HostName")
            .description("Hostname of the search backend")
            //set to false for the time being
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor STORAGE_BACKEND_DIR = new PropertyDescriptor.Builder()
            .name("Storage backend directory")
            .description("Storage backend directory")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor VERTEX_PROPERTY_NAME = new PropertyDescriptor.Builder()
            .name("Vertex Property Name")
            .description("Vertex Property Name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected StandardTitanGraph titanGraph;
    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    public void load(String jsonString, String vPropertyName, ProcessSession session) throws IOException, JSONException {

        session.adjustCounter("Incoming files processed", 1, true);
        JSONObject jsonObject = new JSONObject(jsonString);

        TitanTransaction transaction = null;

        try {

            transaction = titanGraph.newTransaction();
            GraphTraversalSource g = transaction.traversal();

            Vertex fromNode = g.V().has(vPropertyName, jsonObject.getString("origin")).next();
            Vertex toNode = g.V().has(vPropertyName, jsonObject.getString("destination")).next();

            if (fromNode == null) {
                session.adjustCounter("From node not found", 1, true);
            }

            if (toNode == null) {
                session.adjustCounter("To node not found", 1, true);
            }

            if (fromNode != null && toNode != null) {

                String time = jsonObject.getString("time");
                String date = jsonObject.getString("date");
                String unitType = jsonObject.getString("unit_type");
                String headcode = jsonObject.getString("headcode");
                String origin = jsonObject.getString("origin");
                String destination = jsonObject.getString("destination");


                //We are just adding a counter in case the edge attached to fromNode is not null
                if (g.V(fromNode).out("allocation") != null) {
                    session.adjustCounter("The edge connecting fromNode with toNode already exists", 1, true);
                }

                // Check if the edge exists already - this isnt correct yet.
                // TODO: I think we need to add the resource group id to the edge, so
                // we can tell if the allocation has been added already. A headcode can have
                // multiple units allocated. I dont know how we deal with a unit being removed.....
                // If we've already written it in, should we check if its been allocated else where perhaps.
                List properties = new ArrayList();
                g.V(fromNode).outE("allocation").has("time", time).has("date", date).has("hcode", headcode).fill(properties);

                if (properties.size() == 0) {

                    session.adjustCounter("Edge written to Titan", 1, true);
                    fromNode.addEdge("allocation", toNode,
                            "time", time,
                            "date", date,
                            "unit_type", unitType,
                            "hcode", headcode,
                            "origin", origin,
                            "destination", destination);
                }

            } else {
                session.adjustCounter("Failed to write edge to Titan", 1, true);
            }

            transaction.commit();
            transaction.close();

        } catch (Exception ex) {
            session.adjustCounter("Titan transaction failed", 1, true);
            if (transaction != null) {
                transaction.rollback();
            }
            throw new IOException(ex.toString());
        }
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(VERTEX_PROPERTY_NAME);
        descriptors.add(STORAGE_BACKEND);
        descriptors.add(STORAGE_HOSTNAME);
        descriptors.add(STORAGE_BACKEND_DIR);
        descriptors.add(INDEX_SEARCH_BACKEND);
        descriptors.add(INDEX_SEARCH_HOSTNAME);

        this.descriptors = Collections.unmodifiableList(descriptors);

        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {

        ProcessorLog log = getLogger();

        String storageBackend = context.getProperty(STORAGE_BACKEND).getValue();
        String storageHostname = context.getProperty(STORAGE_HOSTNAME).getValue();
        String storageDir = context.getProperty(STORAGE_BACKEND_DIR).getValue();
        String indexBackend = context.getProperty(INDEX_SEARCH_BACKEND).getValue();
        String indexHostname = context.getProperty(INDEX_SEARCH_HOSTNAME).getValue();

        try {
            //Using Titan Native Storage
            BaseConfiguration baseConfiguration = new BaseConfiguration();

            if (storageBackend != null) {
                baseConfiguration.setProperty("storage.backend", storageBackend);
            }

            if (storageHostname != null) {
                baseConfiguration.setProperty("storage.hostname", storageHostname);
            }

            if (indexBackend != null) {
                baseConfiguration.setProperty("index.search.backend", indexBackend);
            }

            if (indexHostname != null) {
                baseConfiguration.setProperty("index.search.hostname", indexBackend);
            }

            if (storageDir != null) {
                baseConfiguration.setProperty("storage.directory", storageDir);
            }

            if (storageBackend == null) {
                throw new IOException("Require at least a storage backend configuration");
            }

            titanGraph = createTitanGraph(baseConfiguration);

        } catch (Exception e) {
            log.error("Invalid Titan Configurations combination", e);
            throw new IOException("Titan Configuration Exception: " + e.getMessage());
        }
    }

    public StandardTitanGraph createTitanGraph(BaseConfiguration baseConfiguration) {
        return (StandardTitanGraph) TitanFactory.open(baseConfiguration);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();
        if (flowFile == null) return;

        String vPropertyName = context.getProperty(VERTEX_PROPERTY_NAME).getValue();

        final AtomicReference<String> holder = new AtomicReference<>();

        session.read(flowFile, new InputStreamCallback() {

            @Override
            public void process(InputStream in) throws IOException {

                StringWriter strWriter = new StringWriter();
                IOUtils.copy(in, strWriter, "UTF-8");
                String contents = strWriter.toString();
                holder.set(contents);
            }

        });

        try {
            load(holder.get(), vPropertyName, session);
            session.transfer(flowFile, SUCCESS);
        } catch (Exception e) {
            session.transfer(flowFile, FAILURE);
            context.yield();
        }
    }

    @OnShutdown
    public void shutdown() {
        titanGraph.close();
    }

}
