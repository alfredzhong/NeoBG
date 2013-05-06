package neo4j;

import java.util.HashMap;
import java.util.Iterator;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.tooling.GlobalGraphOperations;

import edu.usc.bg.base.ByteIterator;

public class Neo4jTest {
	public static void main(String[] args) {
		EmbeddedNeo4jWithIndexing db = new EmbeddedNeo4jWithIndexing(
				"/home/az/course/cs227/graph_databases");
		db.printDBSize();
		db.printRelationSize();
		
	}
}
