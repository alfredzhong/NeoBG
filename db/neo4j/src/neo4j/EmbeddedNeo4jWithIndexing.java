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

public class EmbeddedNeo4jWithIndexing {
	// private static String DB_PATH =
	// "/home/az/course/cs227/graph_databases/graph_test";
	public static String DB_PATH; //
	public static final String USERNAME_KEY = "username";
	public static GraphDatabaseService graphDb;
	public static Index<Node> nodeIndex = null;
	public static Index<Relationship> resourcesIndex = null;
	public static Index<Relationship> friendshipIndex = null;

	// // START SNIPPET: createRelTypes
	// public static enum RelTypes implements RelationshipType {
	// RESOURCE
	// }

	// END SNIPPET: createRelTypes

	// constructor
	public EmbeddedNeo4jWithIndexing(final String db_path) {
		DB_PATH = db_path;
		graphDb = new EmbeddedGraphDatabase(DB_PATH);
		setIndex();
	}

	public void setDbPath(final String db_path) {
		DB_PATH = db_path;
		graphDb = new EmbeddedGraphDatabase(DB_PATH);
		setIndex();
	}

	public void setIndex() {
		if (graphDb != null) {
			nodeIndex = graphDb.index().forNodes("nodes");
			resourcesIndex = graphDb.index().forRelationships("resources");
			friendshipIndex = graphDb.index().forRelationships("friendship");
		}
	}

	// public void populate() {
	// if (DB_PATH == null) {
	// System.err
	// .println("Warning: didn't set data path path yet. Exiting...");
	// return;
	// }
	//
	// System.out.println("Java runtime maximum memory: "
	// + Runtime.getRuntime().maxMemory() / 1024 / 1024 + " Mb.");
	//
	// // START SNIPPET: startDb
	// //graphDb = new EmbeddedGraphDatabase(DB_PATH);
	//
	// //nodeIndex = graphDb.index().forNodes("nodes");
	//
	// // registerShutdownHook();
	// // END SNIPPET: startDb
	//
	// // START SNIPPET: addUsers
	// Node userNode = null;
	// Node usersReferenceNode = null;
	//
	// // transaction, get a user node, and print number of nodes and relations
	// // in the graph
	// Transaction tx = graphDb.beginTx();
	// try {
	// // if the reference node (like root of a graphDB, has a
	// // USERS_REFERENCE type relation,
	// //
	// // then Get the end node that in the 1st relation from the reference
	// // node
	// //
	// // else, create a user node, make a relation from reference to it.
	// if (graphDb.getReferenceNode().hasRelationship(
	// RelTypes.USERS_REFERENCE)) {
	// Iterator<Relationship> itr = graphDb.getReferenceNode()
	// .getRelationships(RelTypes.USERS_REFERENCE).iterator();
	// usersReferenceNode = itr.next().getEndNode();
	// } else {
	// usersReferenceNode = graphDb.createNode();
	// graphDb.getReferenceNode().createRelationshipTo(
	// usersReferenceNode, RelTypes.USERS_REFERENCE);
	// }
	// printDBSize();
	// printRelationSize();
	// tx.success();
	// } catch (Exception e) {
	// tx.failure();
	// } finally {
	// tx.finish();
	// }
	//
	// //
	// // transaction
	// tx = null;
	//
	// // for benchmark
	// long t1 = System.currentTimeMillis();
	// for (int i = 0; i < 10; i++) {
	// System.out.println("Java runtime total memory: "
	// + Runtime.getRuntime().totalMemory() / 1024 / 1024 + "Mb");
	//
	// tx = graphDb.beginTx();
	// try {
	// for (int id = 0; id < 10; id++) {
	// userNode = createAndIndexUser(idToUserName(id));
	// usersReferenceNode.createRelationshipTo(userNode,
	// RelTypes.USER);
	// userNode = null;
	// }
	// tx.success();
	// } catch (Exception e) {
	// tx.failure();
	// } finally {
	// tx.finish();
	// }
	//
	// tx = null;
	// printDBSize();
	// printRelationSize();
	// System.out.println();
	//
	// }
	// // tx = graphDb.beginTx();
	// // Node Node1 = createAndIndexUser(idToUserName(1));
	// // Node Node2 = createAndIndexUser(idToUserName(2));
	// // Node node1=nodeIndex.get(USERNAME_KEY, "user" + 1 +
	// // "@neo4j.org").getSingle();
	// // Node node2=nodeIndex.get(USERNAME_KEY, "user" + 2 +
	// // "@neo4j.org").getSingle();
	// // node1.createRelationshipTo(node2, RelTypes.USER);
	// // tx.success();
	// // tx.finish();
	// // tx=null;
	// // tx.success();
	// // tx.finish();
	// long t2 = System.currentTimeMillis();
	//
	// // result
	// System.out.println("It took " + (t2 - t1) + " ms");
	// // shutdown();
	// }

	public void shutdown() {
		graphDb.shutdown();
	}

	// START SNIPPET: helperMethods
	public static String idToUserName(final int id) {
		// return "user" + id + "@neo4j.org";
		return Integer.toString(id);
	}

	//
	// public static Node createAndIndexUser(final UserProperties
	// userProperties) {
	// Node node = graphDb.createNode();
	//
	// //index user id
	// node.setProperty("userid", idToUserName(userProperties.userid));
	// nodeIndex.add(node, "userid", idToUserName(userProperties.userid));
	//
	// node.setProperty("usename", userProperties.username);
	// node.setProperty("pw", userProperties.pw);
	// node.setProperty("fname", userProperties.fname);
	// node.setProperty("lname", userProperties.lname);
	// node.setProperty("gender", userProperties.gender);
	// node.setProperty("dob", userProperties.dob);
	// node.setProperty("jdate", userProperties.jdate);
	// node.setProperty("ldate", userProperties.ldate);
	// node.setProperty("address", userProperties.address);
	// node.setProperty("email", userProperties.email);
	// node.setProperty("tel", userProperties.tel);
	//
	// return node;
	// }

	public static Node createAndIndexUser(String entityPK,
			HashMap<String, ByteIterator> values, boolean insertImage) {
		Node node = null;
		Transaction tx = graphDb.beginTx();
		try {
			node = graphDb.createNode();

			// index user id
			node.setProperty("userid", entityPK);
			nodeIndex.add(node, "userid", entityPK);

			for (String k : values.keySet()) {
				// if there are no images
				if (!(k.toString().equalsIgnoreCase("pic") || k.toString()
						.equalsIgnoreCase("tpic"))) {
					node.setProperty(k, values.get(k).toString());
				}
			}
			tx.success();
		} finally {
			tx.finish();
		}

		return node;
	}

	public static Relationship createResource(Node n1, Node n2,
			HashMap<String, ByteIterator> values) {
		Relationship resource_edge = null;
		Transaction tx = graphDb.beginTx();
		try {
			// create resource
			resource_edge = n1.createRelationshipTo(n2, RelTypes.RESOURCE);

			for (String k : values.keySet()) {
				// if there are no images
				resource_edge.setProperty(k, values.get(k).toString());
			}

			resourcesIndex.add(resource_edge, "ids", n1.getProperty("userid")
					+ " " + n2.getProperty("userid"));

			// resourcesIndex.add(resource_edge, values);
			tx.success();
		} finally {
			tx.finish();
		}
		return resource_edge;
	}

	// END SNIPPET: helperMethods
	/*
	 * private static void registerShutdownHook() { // Registers a shutdown hook
	 * for the Neo4j and index service instances // so that it shuts down nicely
	 * when the VM exits (even if you // "Ctrl-C" the running example before
	 * it's completed) Runtime.getRuntime().addShutdownHook(new Thread() {
	 * 
	 * @Override public void run() { shutdown(); } }); }
	 */

	// graph object -> global graph operator -> iterator of nodes or relations
	public static void printDBSize() {
		GlobalGraphOperations g = GlobalGraphOperations.at(graphDb);
		Iterator<Node> itr = g.getAllNodes().iterator();
		int count = 0;
		while (itr.hasNext()) {
			itr.next();
			count++;
		}
		System.out.println("Size of Graph DB (number of nodes): " + count);
	}

	public static void printRelationSize() {
		GlobalGraphOperations g = GlobalGraphOperations.at(graphDb);
		Iterator<Relationship> itr = g.getAllRelationships().iterator();
		int count = 0;
		while (itr.hasNext()) {
			itr.next();
			count++;
		}
		System.out.println("Size of Relationships (number of edges): " + count);
	}
}
