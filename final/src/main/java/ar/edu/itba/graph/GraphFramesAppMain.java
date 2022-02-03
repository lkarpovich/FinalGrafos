package ar.edu.itba.graph;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;
import org.w3c.dom.*;
import org.xml.sax.SAXException;
import org.apache.hadoop.conf.Configuration;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import static org.apache.spark.sql.functions.array;

public class GraphFramesAppMain {

    public static void main(String[] args) throws IOException, ParserConfigurationException, SAXException {

        SparkConf spark = new SparkConf().setAppName("test query 1");
        JavaSparkContext sparkContext= new JavaSparkContext(spark);
        SparkSession session = SparkSession.builder()
                .sparkContext(sparkContext.sc())
                .getOrCreate();

        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(session);
        List<Row> vertices = new ArrayList<>();
        List<Row> edges = new ArrayList<>();

        // Read from .graphml file
        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(config);
        FSDataInputStream data = fs.open(new Path(args[0]));

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document dom = db.parse(data);

        NodeList nl = dom.getElementsByTagName("node");
        NodeList el = dom.getElementsByTagName("edge");

        for (int i = 0; i < nl.getLength(); i++) {
            Node node = nl.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE)
                vertices.add(generateVertex(node));
        }

        for (int i = 0; i < el.getLength(); i++) {
            Node node = el.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE)
                edges.add(generateEdge(node));
        }

        Dataset<Row> verticesDF = sqlContext.createDataFrame(vertices, LoadSchemaVertices());
        Dataset<Row> edgesDF = sqlContext.createDataFrame(edges, LoadSchemaEdges() );
        GraphFrame myGraph = GraphFrame.apply(verticesDF, edgesDF);

        //Print graph
        System.out.println("Graph:");
        myGraph.vertices().show();
        myGraph.edges().show();

        System.out.println("Schema");
        myGraph.vertices().printSchema();
        myGraph.edges().printSchema();

        System.out.println("Degree");
        myGraph.degrees().show();

        System.out.println("Indegree");
        myGraph.inDegrees().show();

        System.out.println("Outdegree");
        myGraph.outDegrees().show();

        //print
        final String TIMESTAMP = DateTimeFormatter.ofPattern("yyyy-MM-dd-HHmmss").format(LocalDateTime.now());
        final String DIR_LOC = new Path(args[0]).getParent().toString();
        final String OUTPUT_PATH = "hdfs:///user/lkarpovich/" + TIMESTAMP;


        myGraph.vertices().createOrReplaceTempView("vertex");
        myGraph.edges().createOrReplaceTempView("edges");

        Dataset<Row> withoutStop = myGraph
                .filterEdges("labelE = 'route'")
                .filterVertices("labelV = 'airport'")
                .find("(a)-[e]->(b)")
                .filter("b.code = 'SEA'")
                .filter("a.lat < 0")
                .filter("a.lon < 0")
                .select(array("a.code","b.code").as("route"));

        Dataset<Row> withStop = myGraph
                .filterEdges("labelE = 'route'")
                .filterVertices("labelV = 'airport'")
                .find("(a)-[e]->(b); (b)-[e2]->(c)")
                .filter("c.code = 'SEA'")
                .filter("a.lat < 0")
                .filter("a.lon < 0")
                .select(array("a.code","b.code", "c.code").as("route"));

        withoutStop.union(withStop).show();

        withoutStop.union(withStop).rdd().saveAsTextFile(OUTPUT_PATH + "-b1");
        sparkContext.close();
    }

    public static Row generateVertex(Node node) {
        Vertex v = new Vertex();
        v.setId(Long.valueOf(node.getAttributes().getNamedItem("id").getNodeValue()));

        NodeList data = ((Element)node).getElementsByTagName("*");

        for(int i = 0; i < data.getLength(); i++) {
            Node child = data.item(i);
            NamedNodeMap childAt = child.getAttributes();

            String key = childAt.item(0).getNodeValue();
            String value = child.getTextContent();
            switch (key) {
                case "type":
                    v.setType(value);
                    break;
                case "code":
                    v.setCode(value);
                    break;
                case "icao":
                    v.setIcao(value);
                    break;
                case "desc":
                    v.setDesc(value);
                    break;
                case "region":
                    v.setRegion(value);
                    break;
                case "runways":
                    v.setRunways(Integer.valueOf(value));
                    break;
                case "longest":
                    v.setLongest(Integer.valueOf(value));
                    break;
                case "elev":
                    v.setElev(Integer.valueOf(value));
                    break;
                case "country":
                    v.setCountry(value);
                    break;
                case "city":
                    v.setCity(value);
                    break;
                case "lat":
                    v.setLat(Double.valueOf(value));
                    break;
                case "lon":
                    v.setLon(Double.valueOf(value));
                    break;
                case "author":
                    v.setAuthor(value);
                    break;
                case "date":
                    v.setDate(value);
                    break;
                case "labelV":
                    v.setLabelV(value);
                    break;
                default:
                    System.out.println("unidentified attribute for vertex");

            }
        }
        return RowFactory.create(v.getId(), v.getType(), v.getCode(), v.getIcao(), v.getDesc(), v.getRegion(),
                v.getRunways(), v.getLongest(), v.getElev(), v.getCountry(), v.getCity(), v.getLat(), v.getLon(),
                v.getAuthor(), v.getDate(), v.getLabelV());
    }

    public static Row generateEdge(Node node) {
        Edge edge = new Edge();

        NamedNodeMap atts = node.getAttributes();

        edge.setId(Long.valueOf(atts.getNamedItem("id").getNodeValue()));
        edge.setSrc(Long.valueOf(atts.getNamedItem("source").getNodeValue()));
        edge.setDes(Long.valueOf(atts.getNamedItem("target").getNodeValue()));

        NodeList data = ((Element)node).getElementsByTagName("*");

        for(int i = 0; i < data.getLength(); i++) {
            Node child = data.item(i);
            NamedNodeMap childAt = child.getAttributes();
            String key = childAt.item(0).getNodeValue();
            String value = child.getTextContent();
            switch(key) {
                case "labelE":
                    edge.setLabelE(value);
                    break;
                case "dist":
                    edge.setDist(Integer.valueOf(value));
                    break;
                default:
                    System.out.println("unidentified attribute for edge");
            }
        }
        //TODO decide add id to edge
        return RowFactory.create(edge.getId(), edge.getSrc(), edge.getDes(), edge.getLabelE(), edge.getDist());
    }

    public static StructType LoadSchemaVertices() {
        List<StructField> vertFields = new ArrayList<>();
        vertFields.add(DataTypes.createStructField("id",DataTypes.LongType, true));
        vertFields.add(DataTypes.createStructField("type",DataTypes.StringType, true));
        vertFields.add(DataTypes.createStructField("code",DataTypes.StringType, true));
        vertFields.add(DataTypes.createStructField("icao",DataTypes.StringType, true));
        vertFields.add(DataTypes.createStructField("desc",DataTypes.StringType, true));
        vertFields.add(DataTypes.createStructField("region",DataTypes.StringType, true));
        vertFields.add(DataTypes.createStructField("runways",DataTypes.IntegerType, true));
        vertFields.add(DataTypes.createStructField("longest",DataTypes.IntegerType, true));
        vertFields.add(DataTypes.createStructField("elev",DataTypes.IntegerType, true));
        vertFields.add(DataTypes.createStructField("country",DataTypes.StringType, true));
        vertFields.add(DataTypes.createStructField("city",DataTypes.StringType, true));
        vertFields.add(DataTypes.createStructField("lat",DataTypes.DoubleType, true));
        vertFields.add(DataTypes.createStructField("lon",DataTypes.DoubleType, true));
        vertFields.add(DataTypes.createStructField("author",DataTypes.StringType, true));
        vertFields.add(DataTypes.createStructField("date",DataTypes.StringType, true));
        vertFields.add(DataTypes.createStructField("labelV",DataTypes.StringType, true));

        return DataTypes.createStructType(vertFields);
    }

    public static StructType LoadSchemaEdges() {
        List<StructField> edgeFields = new ArrayList<>();
        edgeFields.add(DataTypes.createStructField("id",DataTypes.LongType, true));
        edgeFields.add(DataTypes.createStructField("src",DataTypes.LongType, false));
        edgeFields.add(DataTypes.createStructField("dst",DataTypes.LongType, false));
        edgeFields.add(DataTypes.createStructField("labelE",DataTypes.StringType, false));
        edgeFields.add(DataTypes.createStructField("dist",DataTypes.IntegerType, true));

        return DataTypes.createStructType(edgeFields);
    }

}
