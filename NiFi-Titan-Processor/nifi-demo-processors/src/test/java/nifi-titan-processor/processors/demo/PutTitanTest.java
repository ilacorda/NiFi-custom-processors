import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PutTitanTest {

    private final static String TEST_STORAGE_BACKEND = "inmemory";
    private static String VERTEX_PROPERTY_NAME = "tiploc";

    private static String successJson = "{\n" +
            "  \"headcode\": \"2F52\",\n" +
            "  \"time\": \"13:00\",\n" +
            "  \"origin\": \"YRKTL\",\n" +
            "  \"destination\": \"GLCTL\",\n" +
            "  \"unit_type\": \"142/0\",\n" +
            "  \"date\": \"2015/04/13\"\n" +
            "}";

    private static String failureJson = "{\n" +
            "  \"headcode\": \"2F52\",\n" +
            "  \"time\": \"13:00\",\n" +
            "  \"origin\": \"NOTTL\",\n" +
            "  \"destination\": \"GUITL\",\n" +
            "  \"unit_type\": \"142/0\",\n" +
            "  \"date\": \"2015/04/13\"\n" +
            "}";

    private MockPutTitan mockPutTitan;

    @Before
    public void setup() {
        mockPutTitan = new MockPutTitan();
    }

    @Test
    public void deserialiseJsonTest() throws JSONException {

        JSONObject jsonObject = new JSONObject(successJson);

        //To check whether the values for the keys in the JSON are those expected
        assertEquals("2F52", jsonObject.getString("headcode"));
        assertEquals("13:00", jsonObject.getString("time"));
        assertEquals("YRKTL", jsonObject.getString("origin"));
        assertEquals("GLCTL", jsonObject.getString("destination"));
        assertEquals("142/0", jsonObject.getString("unit_type"));
        assertEquals("2015/04/13", jsonObject.getString("date"));
    }

    @Test
    public void success() throws Exception {

        //Content to be a mock file
        InputStream content = new ByteArrayInputStream(successJson.getBytes());

        if (content.toString().isEmpty()) {
            System.out.println("The content of the InputStream as converted into a String is empty");
        }

        // Generate a test runner to mock a processor in a flow
        TestRunner testRunner = TestRunners.newTestRunner(mockPutTitan);
        testRunner.setProperty(PutTitan.STORAGE_BACKEND, TEST_STORAGE_BACKEND);
        testRunner.setProperty(PutTitan.VERTEX_PROPERTY_NAME, VERTEX_PROPERTY_NAME);

        //add the content to the runner - performs one operation at the moment
        testRunner.enqueue(content);

        //Run the enqueue content. It can take multiple contents queued
        testRunner.run(1);

        // All results were processed with out failure
        testRunner.assertQueueEmpty();

        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(PutTitan.SUCCESS);
        // Check we have at least one success
        assertTrue("1 match", results.size() == 1);
        // Check the flowfile contents look correct
        MockFlowFile result = results.get(0);
        String resultValue = new String(testRunner.getContentAsByteArray(result));

        JSONObject jsonObject = new JSONObject(resultValue);

        //To check whether the values for the keys in the JSON are those expected
        assertEquals("2F52", jsonObject.getString("headcode"));
        assertEquals("13:00", jsonObject.getString("time"));
        assertEquals("YRKTL", jsonObject.getString("origin"));
        assertEquals("GLCTL", jsonObject.getString("destination"));
        assertEquals("142/0", jsonObject.getString("unit_type"));
        assertEquals("2015/04/13", jsonObject.getString("date"));

        // Check the edge was written correctly
        List properties = new ArrayList();
        GraphTraversalSource g = mockPutTitan.getGraph().traversal();
        g.E().hasLabel("allocation").valueMap("time", "date", "unit_type", "hcode").dedup().fill(properties);
        assertEquals(1, properties.size());

        for (Object obj : properties) {
            HashMap next = (HashMap) obj;
            assertEquals("142/0", next.get("unit_type"));
            assertEquals("2015/04/13", next.get("date"));
            assertEquals("13:00", next.get("time"));
            assertEquals("2F52", next.get("hcode"));
        }
    }

    @Test
    public void failure() throws Exception {

        //Content to be a mock file
        InputStream content = new ByteArrayInputStream(successJson.getBytes());

        if (content.toString().isEmpty()) {
            System.out.println("The content of the InputStream as converted into a String is empty");
        }

        // Generate a test runner to mock a processor in a flow
        TestRunner testRunner = TestRunners.newTestRunner(mockPutTitan);
        testRunner.setProperty(PutTitan.STORAGE_BACKEND, TEST_STORAGE_BACKEND);
        testRunner.setProperty(PutTitan.VERTEX_PROPERTY_NAME, VERTEX_PROPERTY_NAME);

        //add the content to the runner - performs one operation at the moment
        testRunner.enqueue(content);

        //Run the enqueue content. It can take multiple contents queued
        testRunner.run(1);

        // All results were processed with out failure
        testRunner.assertQueueEmpty();

        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(PutTitan.FAILURE);
        System.out.println("This prints the size of the MockFlowFile" + " " + results.size());
        assertTrue("0 match", results.size() == 0);
    }

    @Test
    public void noVertexFound() throws Exception {

        //Content to be a mock file
        InputStream content = new ByteArrayInputStream(failureJson.getBytes());

        if (content.toString().isEmpty()) {
            System.out.println("The content of the InputStream as converted into a String is empty");
        }

        // Generate a test runner to mock a processor in a flow
        TestRunner testRunner = TestRunners.newTestRunner(mockPutTitan);
        testRunner.setProperty(PutTitan.STORAGE_BACKEND, TEST_STORAGE_BACKEND);
        testRunner.setProperty(PutTitan.VERTEX_PROPERTY_NAME, VERTEX_PROPERTY_NAME);

        //add the content to the runner - performs one operation at the moment
        testRunner.enqueue(content);

        //Run the enqueue content. It can take multiple contents queued
        testRunner.run(1);

        // All results were processed with out failure
        testRunner.assertQueueEmpty();

        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(PutTitan.FAILURE);
        assertTrue("1 match", results.size() == 1);
    }
}

