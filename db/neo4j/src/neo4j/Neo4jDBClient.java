/**                                                                                                                                                                                
 * Copyright (c) 2012 USC Database Laboratory All rights reserved. 
 *
 * Authors:  Sumita Barahmand and Shahram Ghandeharizadeh                                                                                                                            
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */

package neo4j;

//import EmbeddedNeo4jWithIndexing;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.ObjectByteIterator;
import edu.usc.bg.workloads.FriendshipWorkload;

import net.sf.ehcache.management.ResourceClassLoader;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.IndexHits;

public class Neo4jDBClient extends DB implements Neo4jConstraints {

	// public static HashSet<String> tables = null;

	/** The code to return when the call succeeds. **/
	public static final int SUCCESS = 0;
	/** The code to return when the call fails. **/
	public static final int ERROR = -1;

	private static String FSimagePath = "";

	private static boolean initialized = false;

	// private boolean verbose = false;
	private Properties props;

	private static final String DEFAULT_PROP = "/home/az/course/cs227/graph_databases/graph_test";

	// neo4j client members
	private EmbeddedNeo4jWithIndexing db;

	public static void main(String[] args) {
		// TestImage();
		// TestUnit();
		// PostgreDBClient pdb = new PostgreDBClient();
		// try {
		// pdb.init();
		// pdb.createSchema(null);
		// } catch (DBException e) {
		// e.printStackTrace();
		// }
	}

	/**
	 * Test stub for Postgresql database client.
	 */
	public static void TestUnit() {
		Neo4jDBClient pgdb = new Neo4jDBClient();
		try {
			pgdb.init();
			pgdb.buildIndexes(null);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	/**
	 * Test stub for storing and retrieving images from file system.
	 */
	public static void TestImage() {
		String userid = "0001";
		byte[] image = new byte[100];
		boolean profileimg = true;
		Neo4jDBClient pg = new Neo4jDBClient();
		pg.StoreImageInFS(userid, image, profileimg);
		pg.GetImageFromFS(userid, profileimg);
	}

	private boolean StoreImageInFS(String userid, byte[] image,
			boolean profileimg) {
		boolean result = true;
		String text = "thumbnail";

		if (profileimg)
			text = "profile";
		String ImageFileName = FSimagePath + File.separator + "img" + userid
				+ text;

		File tgt = new File(ImageFileName);
		if (tgt.exists()) {
			if (!tgt.delete()) {
				System.out.println("Error, file exists and failed to delete");
				return false;
			}
		}

		// Write the file.
		try {
			FileOutputStream fos = new FileOutputStream(ImageFileName);
			fos.write(image);
			fos.close();
		} catch (Exception ex) {
			System.out.println("Error in writing the file " + ImageFileName);
			ex.printStackTrace(System.out);
		}

		return result;
	}

	private byte[] GetImageFromFS(String userid, boolean profileimg) {
		int filelength = 0;
		String text = "thumbnail";
		byte[] imgpayload = null;

		if (profileimg)
			text = "profile";

		String ImageFileName = FSimagePath + File.separator + "img" + userid
				+ text;
		int attempt = 100;
		while (attempt > 0) {
			try {
				FileInputStream fis = null;
				DataInputStream dis = null;
				File fsimage = new File(ImageFileName);
				filelength = (int) fsimage.length();
				imgpayload = new byte[filelength];
				fis = new FileInputStream(fsimage);
				dis = new DataInputStream(fis);
				int read = 0;
				int numRead = 0;
				while (read < filelength
						&& (numRead = dis.read(imgpayload, read, filelength
								- read)) >= 0) {
					read = read + numRead;
				}
				dis.close();
				fis.close();
				break;
			} catch (IOException e) {
				e.printStackTrace(System.out);
				--attempt;
			}
		}

		return imgpayload;
	}

	/**
	 * Initialize the database connection and set it up for sending requests to
	 * the database. This must be called once per client.
	 * 
	 */
	@Override
	public boolean init() throws DBException {
		System.out.println("init, and initialized=" + initialized);
		if (initialized == true) {
			System.out.println("Client connection already initialized.");
			return true;
		}
		//

		props = getProperties();
		String urls = props.getProperty(Neo4jConstraints.NEO4J_URL_PROPERTY,
				DEFAULT_PROP);
		db = new EmbeddedNeo4jWithIndexing(urls);

		initialized = true;
		return true;
	}

	@Override
	public void cleanup(boolean warmup) {
		// db.shutdown();
	}

	

	// Loading phase query will not be cached.
	@Override
	public int insertEntity(String entitySet, String entityPK,
			HashMap<String, ByteIterator> values, boolean insertImage) {

		

		if (null == entitySet || null == entityPK)
			return ERROR;

		if (entitySet.equalsIgnoreCase("users")) {

			db.createAndIndexUser(entityPK, values, insertImage);

		} else if (entitySet.equalsIgnoreCase("resources")) {

			// find creator node for resource
			// db.setIndex();
			IndexHits<Node> result = EmbeddedNeo4jWithIndexing.nodeIndex.get(
					"userid", values.get("creatorid").toString());

			// make sure primary key of user only return 1 user node
			if (result.size() != 1) {
				System.err
						.println("Error: in Neo4jDBClient.insertEntity, primary key userid ="
								+ values.get("creatorid").toString()
								+ "returns multiple nodes. Exiting...");
				System.exit(-1);
			}

			Node creatorNode = result.iterator().next();

			// find wallusernode for resource
			result = EmbeddedNeo4jWithIndexing.nodeIndex.get("userid", values
					.get("walluserid").toString());

			// make sure primary key of user only return 1 user node
			if (result.size() != 1) {
				System.err
						.println("Error: in Neo4jDBClient.insertEntity, primary key userid ="
								+ values.get("creatorid").toString()
								+ "returns multiple nodes. Exiting...");
				System.exit(-1);
			}

			Node walluserNode = result.iterator().next();

			HashMap<String, ByteIterator> propertyToSet = new HashMap<String, ByteIterator>();
			for (String k : values.keySet()) {
				if ((!k.equals("creatorid")) && (!k.equals("walluserid"))) {
					propertyToSet.put(k, values.get(k));
				}
			}
			// create resource
			Relationship resource_edge = db.createResource(creatorNode,
					walluserNode, propertyToSet);

			// add userids index to the edge index resourcesIndex
			Transaction tx = db.graphDb.beginTx();
			try {
				resource_edge.setProperty("rid", entityPK);

				db.resourcesIndex.add(
						resource_edge,
						"ids",
						creatorNode.getProperty("userid") + " "
								+ walluserNode.getProperty("userid"));
				tx.success();
			} finally {
				tx.finish();
			}

			// set relation properties, skip creatorid, walluserid

		} else {
			System.err
					.println("Error in Neo4jDBClient.insertEntity: Invalid entitySet: "
							+ entitySet + ", return ERROR.");
			return ERROR;
		}
		db.printDBSize();
		db.printRelationSize();
		return SUCCESS;
	}

	@Override
	public int viewProfile(int requesterID, int profileOwnerID,
			HashMap<String, ByteIterator> result, boolean insertImage,
			boolean testMode) {
		int retVal = SUCCESS;
		String count = null;
		ExecutionEngine engine = new ExecutionEngine(db.graphDb);
		ExecutionResult ret = engine
				.execute("START n=node(*) MATCH (n) -[FRIENDSHIP]->(m) "
						+ "WHERE HAS(n.userid) AND HAS(m.userid) AND HAS(FRIENDSHIP.status) AND "
						+ "(FRIENDSHIP.status='2') AND (n.userid='"
						+ profileOwnerID + "' OR m.userid='" + profileOwnerID
						+ "') " + "RETURN COUNT(FRIENDSHIP)");
		for (Map<String, Object> row : ret) {
			for (Entry<String, Object> col : row.entrySet()) {
				count = col.getValue().toString();
			}
		}
		if (Integer.parseInt(count) == 0) {
			result.put("friendcount", new ObjectByteIterator("0".getBytes()));
		} else {
			result.put("friendcount", new ObjectByteIterator(count.getBytes()));
		}
		if (requesterID == profileOwnerID) {
			ret = engine
					.execute("START n=node(*) MATCH (m)-[FRIENDSHIP]->(n) "
							+ "WHERE HAS(m.userid) AND HAS(n.userid) AND HAS(FRIENDSHIP.status) AND "
							+ "(n.userid='"
							+ profileOwnerID
							+ "') AND (FRIENDSHIP.status='1') RETURN COUNT(FRIENDSHIP)");
			for (Map<String, Object> row : ret) {
				for (Entry<String, Object> col : row.entrySet()) {
					count = col.getValue().toString();
				}
			}
			if (Integer.parseInt(count) == 0) {
				result.put("pendingcount",
						new ObjectByteIterator("0".getBytes()));
			} else {
				result.put("pendingcount",
						new ObjectByteIterator(count.getBytes()));
			}
		}

		ret = engine
				.execute("START n=node(*) MATCH (n)-[RESOURCE]-(m) WHERE HAS(RESOURCE.walluserid) AND "
						+ "(RESOURCE.walluserid='"
						+ profileOwnerID
						+ "') RETURN COUNT(RESOURCE)");
		for (Map<String, Object> row : ret) {
			for (Entry<String, Object> col : row.entrySet()) {
				count = col.getValue().toString();
			}
		}
		if (Integer.parseInt(count) == 0) {
			result.put("resourcecount", new ObjectByteIterator("0".getBytes()));
		} else {
			result.put("resourcecount",
					new ObjectByteIterator(count.getBytes()));
		}
		return retVal;

	}

	@Override
	public int listFriends(int requesterID, int profileOwnerID,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result,
			boolean insertImage, boolean testMode) {
		int retVal = SUCCESS;
		

		ExecutionEngine engine = new ExecutionEngine(db.graphDb);
		ExecutionResult ret = engine
				.execute("START n=node(*) match (n)-[FRIENDSHIP]-(m) WHERE "
						+

						"HAS(n.userid) AND HAS(m.userid) AND HAS(FRIENDSHIP.status) AND "
						+ "((n.userid='"
						+ profileOwnerID
						+ "') OR (m.userid='"
						+ profileOwnerID
						+ "')) AND FRIENDSHIP.status='2' "
						+

						"RETURN n.userid AS userid, m.userid AS invitee, "
						+ "n.username, n.fname, n.lname,n.gender,n.dob,n.jdate,n.ldate,n.address,n.email,n.tel");
		HashMap<String, ByteIterator> m = new HashMap<String, ByteIterator>();
		for (Map<String, Object> row : ret) {
			for (Entry<String, Object> col : row.entrySet()) {
				if (col.getKey().equalsIgnoreCase("userid"))
					m.put("userid", new ObjectByteIterator(col.getValue()
							.toString().getBytes()));
			}
		}
		result.add(m);
		
		return retVal;
	}

	@Override
	public int viewFriendReq(int profileOwnerID,
			Vector<HashMap<String, ByteIterator>> results, boolean insertImage,
			boolean testMode) {
		int retVal = SUCCESS;
		ExecutionEngine engine = new ExecutionEngine(db.graphDb);
		ExecutionResult result = engine
				.execute("START n=node(*) match (n)-[FRIENDSHIP]->(m) WHERE "
						+ "HAS(m.userid) AND HAS(FRIENDSHIP.status) AND "
						+ "(m.userid = '"
						+ profileOwnerID
						+ "') AND FRIENDSHIP.status='1' RETURN n.userid AS userid, m.userid AS invitee,"
						+

						"n.username, n.fname, n.lname,n.gender,n.dob,n.jdate,n.ldate,n.address,n.email,n.tel");

		HashMap<String, ByteIterator> m = new HashMap<String, ByteIterator>();
		for (Map<String, Object> row : result) {

			for (Entry<String, Object> col : row.entrySet()) {
				if (col.getKey().equalsIgnoreCase("userid"))
					m.put("userid", new ObjectByteIterator(col.getValue()
							.toString().getBytes()));
			}
		}
		results.add(m);
		return retVal;
	}

	public static String friendKey(int id1, int id2) {
		return Integer.toString(id1) + " " + Integer.toString(id2);
	}

	@Override
	public int acceptFriend(int inviterID, int inviteeID) {
		if (inviterID < 0 || inviteeID < 0) 
			return ERROR;
		int retVal = SUCCESS;
		ExecutionEngine engine = new ExecutionEngine(db.graphDb);
		ExecutionResult result = engine.execute("START n=node(*), m = node(*) MATCH (n)-[FRIENDSHIP]->(m) WHERE "
				+ "HAS(FRIENDSHIP.status) AND HAS(n.userid) AND HAS(m.userid) AND (n.userid = '"
				+ inviterID
				+ "') "
				+ "AND (m.userid='"
				+ inviteeID
				+ "') SET FRIENDSHIP.status='2'");
		return retVal;
	}

	@Override
	public int rejectFriend(int inviterID, int inviteeID) {

		int retVal = SUCCESS;
		ExecutionEngine engine = new ExecutionEngine(db.graphDb);
		ExecutionResult result = engine.execute("START n=node(*) MATCH (n)-[FRIENDSHIP]->(m) "
				+ "WHERE HAS(n.userid) AND HAS(m.userid) AND HAS(FRIENDSHIP.status) AND FRIENDSHIP.status='1' "
				+ "AND (n.userid='" + inviterID + "') AND (m.userid='"
				+ inviteeID + "') DELETE FRIENDSHIP");
		return retVal;
		
	}

	@Override
	public int inviteFriend(int inviterID, int inviteeID) {
		int retVal = SUCCESS;

		if (inviterID == inviteeID) {
			return retVal;
		}

		// find the current friendship relationship between these two nodes
		Transaction tx = db.graphDb.beginTx();
		try {
			IndexHits<Relationship> result = db.friendshipIndex.get("ids",
					friendKey(inviterID, inviteeID));

			IndexHits<Relationship> result2 = db.friendshipIndex.get("ids",
					friendKey(inviteeID, inviterID));

			if (result.size() == 0 && result2.size() == 0) {
				Node inviter = db.nodeIndex.get("userid",
						Integer.toString(inviterID)).getSingle();
				Node invitee = db.nodeIndex.get("userid",
						Integer.toString(inviteeID)).getSingle();
				Relationship f = inviter.createRelationshipTo(invitee,
						RelTypes.FRIENDSHIP);
				f.setProperty("status", "1");
				db.friendshipIndex.add(f, "ids",
						friendKey(inviterID, inviteeID));
			}
			tx.success();
		} catch (Exception e) {
			tx.failure();
		} finally {
			tx.finish();
		}

		return retVal;
	}

	@Override
	public int viewTopKResources(int requesterID, int profileOwnerID, int k,
			Vector<HashMap<String, ByteIterator>> result) {
		
		int retVal = SUCCESS;
        HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();

        ExecutionEngine engine = new ExecutionEngine(db.graphDb);
        ExecutionResult ret = engine.execute("START n=node(*) MATCH (n)-[RESOURCE]-(m) WHERE " +
                "HAS(RESOURCE.walluserid) AND (RESOURCE.walluserid='"+profileOwnerID+"') " +
                "WITH RESOURCE ORDER BY RESOURCE.rid desc LIMIT " + k + " return RESOURCE.rid, RESOURCE.creatorid, " +
                "RESOURCE.walluserid, RESOURCE.type, RESOURCE.body, RESOURCE.doc");

        for ( Map<String, Object> row : ret ) {
            for ( Entry<String, Object> col : row.entrySet() ) {
                if(col.getKey().equalsIgnoreCase("RESOURCE.rid")) {
                    values.put("rid", new ObjectByteIterator(col.getValue().toString().getBytes()));
                } else if (col.getKey().equalsIgnoreCase("RESOURCE.walluserid")) {
                    values.put("walluserid", new ObjectByteIterator(col.getValue().toString().getBytes()));
                }
            }
        }
        result.add(values);
        return retVal;
	}

	@Override
	public int getCreatedResources(int creatorID,
			Vector<HashMap<String, ByteIterator>> result) {
		int retVal = SUCCESS;
        HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
        ExecutionEngine engine = new ExecutionEngine(db.graphDb);
        ExecutionResult ret = engine.execute("START n=node(*) MATCH (n)-[RESOURCE]-(m) WHERE " +
                "HAS(RESOURCE.creatorid) AND (RESOURCE.creatorid='"+creatorID+"') RETURN RESOURCE.rid AS rid, RESOURCE.creatorid, " +

                "RESOURCE.walluserid, RESOURCE.type, RESOURCE.body, RESOURCE.doc");
        for ( Map<String, Object> row : ret ) {
            for ( Entry<String, Object> col : row.entrySet() ) {
                if(col.getKey().equalsIgnoreCase("rid"))

                    values.put("rid", new ObjectByteIterator(col.getValue().toString().getBytes()));
            }
        }
        result.add(values);
        return retVal;
	}

	@Override
	public int viewCommentOnResource(int requesterID, int profileOwnerID,
			int resourceID, Vector<HashMap<String, ByteIterator>> result) {
		int retVal = SUCCESS;
		

		return retVal;
	}

	@Override
	public int postCommentOnResource(int commentCreatorID, int profileOwnerID,
			int resourceID, HashMap<String, ByteIterator> values) {
		int retVal = SUCCESS;

		

		return retVal;
	}

	@Override
	public int delCommentOnResource(int resourceCreatorID, int resourceID,
			int manipulationID) {
		int retVal = SUCCESS;
		

		return retVal;
	}

	@Override
	public int thawFriendship(int friendid1, int friendid2) {
		int retVal = SUCCESS;

		// Remove index first

		//db.friendshipIndex.remove(db.friendshipIndex.get("ids",
		//		friendKey(friendid1, friendid2)).getSingle());
		System.out.println("--------------------------------");

		ExecutionEngine engine = new ExecutionEngine(db.graphDb);
		ExecutionResult result = engine
				.execute("START n=node(*) MATCH (n) -[FRIENDSHIP]-(m) "
						+ "WHERE HAS(n.userid) AND HAS(m.userid) AND HAS(FRIENDSHIP.status) "
						+ "AND (FRIENDSHIP.status='2') AND " + "(n.userid='"
						+ friendid1 + "') AND (m.userid='" + friendid2
						+ "') DELETE FRIENDSHIP");
		

		return retVal;
	}

	@Override
	public HashMap<String, String> getInitialStats() {
		HashMap<String, String> stats = new HashMap<String, String>();

		stats.put("usercount", Integer.toString(db.size() - 1));

		String offset = "0";

		// String offset = Integer.toString(db.min_userid);
		ExecutionEngine engine = new ExecutionEngine(db.graphDb);
		ExecutionResult result = engine
				.execute("START n=node(*) MATCH (n)-[RESOURCE]->(m) "
						+ "WHERE HAS(n.userid) AND HAS(RESOURCE.rid) AND (n.userid='"
						+ offset + "') return COUNT(RESOURCE.rid)");
		String resourcesperuser = null;
		for (Map<String, Object> row : result) {
			for (Entry<String, Object> col : row.entrySet()) {
				resourcesperuser = col.getValue().toString();
			}
		}
		if (resourcesperuser == null) {
			stats.put("resourcesperuser", "0");
		} else {
			stats.put("resourcesperuser", resourcesperuser);
		}

		result = engine
				.execute("start n=node(*) match (n)-[FRIENDSHIP]-(m) "
						+ "where has(n.userid) AND has(FRIENDSHIP.status) and n.userid='"
						+ offset
						+ "' AND FRIENDSHIP.status='2' return COUNT(FRIENDSHIP)");
		String avgfriendsperuser = null;
		for (Map<String, Object> row : result) {

			for (Entry<String, Object> col : row.entrySet()) {
				avgfriendsperuser = col.getValue().toString();
				
			}
		}

		if (avgfriendsperuser == null) {
			stats.put("avgfriendsperuser", "0");
		} else {
			stats.put("avgfriendsperuser", avgfriendsperuser);
		}

		result = engine
				.execute("start n=node(*) match (n)-[FRIENDSHIP]->(m) "
						+ "where has(m.userid) AND has(FRIENDSHIP.status) and m.userid='"
						+ offset
						+ "' AND FRIENDSHIP.status='1' return COUNT(FRIENDSHIP)");
		String avgpendingperuser = null;
		for (Map<String, Object> row : result) {

			for (Entry<String, Object> col : row.entrySet()) {
				avgpendingperuser = col.getValue().toString();
				
			}
		}

		if (avgpendingperuser == null) {
			stats.put("avgpendingperuser", "0");
		} else {
			stats.put("avgpendingperuser", avgpendingperuser);
		}

		return stats;
	}

	@Override
	public int CreateFriendship(int friendid1, int friendid2) {
		int retVal = SUCCESS;

		// find the inviter
		IndexHits<Node> result = EmbeddedNeo4jWithIndexing.nodeIndex.get(
				"userid", Integer.toString(friendid1));

		// make sure primary key of user only return 1 user node
		if (result.size() != 1) {
			System.err
					.println("Error: in Neo4jDBClient.insertEntity, primary key userid ="
							+ friendid1 + "returns multiple nodes. Exiting...");
			System.exit(-1);
		}

		Node inviter = result.iterator().next();

		result = EmbeddedNeo4jWithIndexing.nodeIndex.get("userid",
				Integer.toString(friendid2));

		if (result.size() != 1) {
			System.err
					.println("Error: in Neo4jDBClient.insertEntity, primary key userid ="
							+ friendid2 + "returns multiple nodes. Exiting...");
			System.exit(-1);
		}

		Node invitee = result.iterator().next();

		Transaction tx = db.graphDb.beginTx();
		try {
			
			Relationship friendship_edge = inviter.createRelationshipTo(
					invitee, RelTypes.FRIENDSHIP);
			friendship_edge.setProperty("status", Integer.toString(2));

			db.friendshipIndex.add(
					friendship_edge,
					"ids",
					inviter.getProperty("userid") + " "
							+ invitee.getProperty("userid"));

			tx.success();

		} finally {
			tx.finish();
		}

		db.printDBSize();
		db.printRelationSize();

		return retVal;
	}

	@Override
	public void createSchema(Properties props) {
		Statement stmt = null;

		
	}

	@Override
	public void buildIndexes(Properties props) {
		
	}

	public static void dropTable(Statement st, String tableName) {
		try {
			st.executeUpdate("DROP TABLE " + tableName);
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}

	public static void dropIndex(Statement st, String indexName) {
		try {
			st.executeUpdate("DROP INDEX " + indexName);
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}

	public static void dropSequence(Statement st, String sequenceName) {
		try {
			st.executeUpdate("DROP SEQUENCE " + sequenceName);
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}

	@Override
	public int queryPendingFriendshipIds(int memberID,
			Vector<Integer> pendingIds) {
		return SUCCESS;
	}

	@Override
	public int queryConfirmedFriendshipIds(int memberID,
			Vector<Integer> confirmedIds) {
		return SUCCESS;
	}

}
