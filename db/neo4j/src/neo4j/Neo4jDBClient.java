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

	// private PreparedStatement createAndCacheStatement(int stmttype, String
	// query) throws SQLException{
	// PreparedStatement newStatement = conn.prepareStatement(query);
	// PreparedStatement stmt = newCachedStatements.putIfAbsent(stmttype,
	// newStatement);
	// if (stmt == null) return newStatement;
	// else return stmt;
	// }

	// Loading phase query will not be cached.
	@Override
	public int insertEntity(String entitySet, String entityPK,
			HashMap<String, ByteIterator> values, boolean insertImage) {

		// for test table name;
		// if (tables == null) {
		// tables = new HashSet<String>();
		// }
		// tables.add(entitySet);
		// System.out.println("Current unique tables: ");
		// for (String e : tables) {
		// System.out.println(e);
		// }
		// System.out.println("\n\n");

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
		// ResultSet rs = null;
		//
		// if (requesterID < 0 || profileOwnerID < 0)
		// return ERROR;
		//
		// String query = "";
		// String uid = "";
		// try {
		// // Friend count.
		// query =
		// "SELECT count(*) FROM friendship WHERE (inviterID = ? OR inviteeID = ?) AND status = 2";
		// if ((preparedStatement = newCachedStatements.get(GETFRNDCNT_STMT)) ==
		// null)
		// preparedStatement = createAndCacheStatement(GETFRNDCNT_STMT, query);
		// preparedStatement.setInt(1, profileOwnerID);
		// preparedStatement.setInt(2, profileOwnerID);
		// rs = preparedStatement.executeQuery();
		// if (rs.next())
		// result.put("friendcount", new
		// ObjectByteIterator(rs.getString(1).getBytes()));
		// else
		// result.put("friendcount", new ObjectByteIterator("0".getBytes()));
		// } catch (SQLException sx) {
		// retVal = ERROR;
		// sx.printStackTrace(System.out);
		// } finally {
		// try {
		// if (rs != null)
		// rs.close();
		// if (preparedStatement != null)
		// preparedStatement.clearParameters();
		// } catch (SQLException e) {
		// e.printStackTrace(System.out);
		// retVal = ERROR;
		// }
		// }
		//
		// // Pending friend request count.
		// // If owner viewing her own profile, she can view her pending friend
		// requests.
		// if (requesterID == profileOwnerID) {
		// query =
		// "SELECT count(*) FROM friendship WHERE inviteeID = ? AND status = 1";
		// try {
		// if ((preparedStatement = newCachedStatements.get(GETPENDCNT_STMT)) ==
		// null)
		// preparedStatement = createAndCacheStatement(GETPENDCNT_STMT, query);
		// preparedStatement.setInt(1, profileOwnerID);
		// rs = preparedStatement.executeQuery();
		// if (rs.next())
		// result.put("pendingcount", new
		// ObjectByteIterator(rs.getString(1).getBytes()));
		// else
		// result.put("pendingcount", new ObjectByteIterator("0".getBytes()));
		// } catch (SQLException sx) {
		// retVal = ERROR;
		// sx.printStackTrace(System.out);
		// } finally {
		// try {
		// if (rs != null)
		// rs.close();
		// if (preparedStatement != null)
		// preparedStatement.clearParameters();
		// } catch (SQLException e) {
		// e.printStackTrace(System.out);
		// retVal = ERROR;
		// }
		// }
		// }
		//
		// // Resource count.
		// query = "SELECT count(*) FROM resources WHERE wallUserID = ?";
		//
		// try {
		// if ((preparedStatement = newCachedStatements.get(GETRESCNT_STMT)) ==
		// null)
		// preparedStatement = createAndCacheStatement(GETRESCNT_STMT, query);
		// preparedStatement.setInt(1, profileOwnerID);
		// rs = preparedStatement.executeQuery();
		// if (rs.next())
		// result.put("resourcecount", new
		// ObjectByteIterator(rs.getString(1).getBytes())) ;
		// else
		// result.put("resourcecount", new ObjectByteIterator("0".getBytes())) ;
		// } catch (SQLException sx) {
		// retVal = ERROR;
		// sx.printStackTrace(System.out);
		// } finally {
		// try {
		// if (rs != null)
		// rs.close();
		// if (preparedStatement != null)
		// preparedStatement.clearParameters();
		// } catch (SQLException e) {
		// retVal = ERROR;
		// e.printStackTrace(System.out);
		// }
		// }
		//
		// // Profile details.
		// try {
		// if (insertImage && FSimagePath.equals("")) {
		// query =
		// "SELECT userid, username, fname, lname, gender, dob, jdate, ldate, address, email, tel, pic FROM users WHERE UserID = ?";
		// if ((preparedStatement = newCachedStatements.get(GETPROFILEIMG_STMT))
		// == null)
		// preparedStatement = createAndCacheStatement(GETPROFILEIMG_STMT,
		// query);
		// } else {
		// query =
		// "SELECT userid, username, fname, lname, gender, dob, jdate, ldate, address, email, tel FROM users WHERE UserID = ?";
		// if ((preparedStatement = newCachedStatements.get(GETPROFILE_STMT)) ==
		// null)
		// preparedStatement = createAndCacheStatement(GETPROFILE_STMT, query);
		// }
		// preparedStatement.setInt(1, profileOwnerID);
		// rs = preparedStatement.executeQuery();
		// ResultSetMetaData md = rs.getMetaData();
		// int col = md.getColumnCount();
		// if (rs.next()) {
		// for (int i = 1; i <= col; i++){
		// String col_name = md.getColumnName(i);
		// String value = "";
		//
		// if (col_name.equalsIgnoreCase("userid")) {
		// uid = rs.getString(col_name);
		// }
		//
		// if (col_name.equalsIgnoreCase("pic") ) {
		// // Get as bytes.
		// byte[] bytes = rs.getBytes(i);
		// value = bytes.toString();
		// // If test mode dump pic into a file.
		// if (testMode) {
		// // Dump to file.
		// try {
		// FileOutputStream fos = new FileOutputStream(profileOwnerID +
		// "-proimage.bmp");
		// fos.write(bytes);
		// fos.close();
		// } catch (Exception ex) {
		// System.out.println(ex.getMessage());
		// }
		// }
		// } else
		// value = rs.getString(col_name);
		// result.put(col_name, new ObjectByteIterator(value.getBytes()));
		// }
		//
		// // Fetch the profile image from the file system.
		// if (insertImage && !FSimagePath.equals("") ) {
		// // Get the profile image from the file.
		// byte[] profileImage = GetImageFromFS(uid, true);
		// if (testMode) {
		// // Dump to file.
		// try {
		// FileOutputStream fos = new FileOutputStream(profileOwnerID +
		// "-proimage.bmp");
		// fos.write(profileImage);
		// fos.close();
		// } catch (Exception ex){
		// }
		// }
		// result.put("pic", new ObjectByteIterator(profileImage));
		// }
		// }
		// } catch (SQLException sx) {
		// retVal = ERROR;
		// sx.printStackTrace(System.out);
		// } finally {
		// try {
		// if (rs != null)
		// rs.close();
		// if(preparedStatement != null)
		// preparedStatement.clearParameters();
		// } catch (SQLException e) {
		// retVal = ERROR;
		// e.printStackTrace(System.out);
		// }
		// }

		return retVal;
	}

	@Override
	public int listFriends(int requesterID, int profileOwnerID,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result,
			boolean insertImage, boolean testMode) {
		int retVal = SUCCESS;
		// ResultSet rs = null;
		//
		// if (requesterID < 0 || profileOwnerID < 0)
		// return ERROR;
		//
		// String query = "";
		// String uid = "";
		// try {
		// if (insertImage && FSimagePath.equals("")) {
		// query =
		// "SELECT userid, inviterid, inviteeid, username, fname, lname, gender, dob, jdate, ldate, address, email, tel, tpic FROM users, friendship WHERE ((inviterid = ? AND userid = inviteeid) or (inviteeid = ? AND userid = inviterid)) AND status = 2";
		// if ((preparedStatement = newCachedStatements.get(GETFRNDSIMG_STMT))
		// == null)
		// preparedStatement = createAndCacheStatement(GETFRNDSIMG_STMT, query);
		// } else {
		// query =
		// "SELECT userid, inviterid, inviteeid, username, fname, lname, gender, dob, jdate, ldate, address, email, tel FROM users, friendship WHERE ((inviterid = ? AND userid = inviteeid) or (inviteeid = ? AND userid = inviterid)) AND status = 2";
		// if ((preparedStatement = newCachedStatements.get(GETFRNDS_STMT)) ==
		// null)
		// preparedStatement = createAndCacheStatement(GETFRNDS_STMT, query);
		// }
		// preparedStatement.setInt(1, profileOwnerID);
		// preparedStatement.setInt(2, profileOwnerID);
		// rs = preparedStatement.executeQuery();
		// int cnt = 0;
		// while (rs.next()) {
		// cnt++;
		// HashMap<String, ByteIterator> values = new HashMap<String,
		// ByteIterator>();
		// if (fields != null) {
		// for (String field : fields) {
		// String value = rs.getString(field);
		// if (field.equalsIgnoreCase("userid"))
		// field = "userid";
		// values.put(field, new ObjectByteIterator(value.getBytes()));
		// }
		// result.add(values);
		// } else {
		// // Get the number of columns and their names.
		// ResultSetMetaData md = rs.getMetaData();
		// int col = md.getColumnCount();
		// for (int i = 1; i <= col; i++) {
		// String col_name = md.getColumnName(i);
		// String value = "";
		// if (col_name.equalsIgnoreCase("tpic")) {
		// // Get as a bytes.
		// byte[] bytes = rs.getBytes(i);
		// value = bytes.toString();
		// if (testMode){
		// // Dump to file.
		// try{
		// FileOutputStream fos = new FileOutputStream(profileOwnerID + "-" +
		// cnt + "-thumbimage.bmp");
		// fos.write(bytes);
		// fos.close();
		// }catch(Exception ex){
		// }
		// }
		// } else {
		// value = rs.getString(col_name);
		// if (col_name.equalsIgnoreCase("userid")) {
		// uid = value;
		// col_name = "userid";
		// }
		// }
		// values.put(col_name, new ObjectByteIterator(value.getBytes()));
		// }
		// // Fetch the thumbnail image from the file system.
		// if (insertImage && !FSimagePath.equals("")) {
		// byte[] thumbImage = GetImageFromFS(uid, false);
		// // Get the thumbnail image from the file.
		// if (testMode) {
		// // Dump to file.
		// try {
		// FileOutputStream fos = new FileOutputStream(profileOwnerID + "-" +
		// cnt + "-thumbimage.bmp");
		// fos.write(thumbImage);
		// fos.close();
		// } catch(Exception ex) {
		// }
		// }
		// values.put("tpic", new ObjectByteIterator(thumbImage));
		// }
		// result.add(values);
		// }
		// }
		// } catch(SQLException sx) {
		// retVal = ERROR;
		// sx.printStackTrace(System.out);
		// } finally {
		// try {
		// if (rs != null)
		// rs.close();
		// if(preparedStatement != null)
		// preparedStatement.clearParameters();
		// } catch (SQLException e) {
		// retVal = ERROR;
		// e.printStackTrace(System.out);
		// }
		// }

		return retVal;
	}

	@Override
	public int viewFriendReq(int profileOwnerID,
			Vector<HashMap<String, ByteIterator>> results, boolean insertImage,
			boolean testMode) {
		int retVal = SUCCESS;
		// ResultSet rs = null;
		// if (profileOwnerID < 0)
		// return ERROR;
		//
		// String query = "";
		// String uid = "";
		// try {
		// if (insertImage && FSimagePath.equals("")) {
		// query =
		// "SELECT userid, inviterid, inviteeid, username, fname, lname, gender, dob, jdate, ldate, address, email, tel, tpic FROM users, friendship WHERE inviteeid = ? AND status = 1 AND inviterid = userid";
		// if ((preparedStatement = newCachedStatements.get(GETPENDIMG_STMT)) ==
		// null)
		// preparedStatement = createAndCacheStatement(GETPENDIMG_STMT, query);
		// } else {
		// query =
		// "SELECT userid, inviterid, inviteeid, username, fname, lname, gender, dob, jdate, ldate, address, email, tel FROM users, friendship WHERE inviteeid = ? AND status = 1 AND inviterid = userid";
		// if ((preparedStatement = newCachedStatements.get(GETPEND_STMT)) ==
		// null)
		// preparedStatement = createAndCacheStatement(GETPEND_STMT, query);
		// }
		// preparedStatement.setInt(1, profileOwnerID);
		// rs = preparedStatement.executeQuery();
		// int cnt = 0;
		// while (rs.next()) {
		// cnt++;
		// HashMap<String, ByteIterator> values = new HashMap<String,
		// ByteIterator>();
		// // Get the number of columns and their names.
		// ResultSetMetaData md = rs.getMetaData();
		// int col = md.getColumnCount();
		// for (int i = 1; i <= col; i++) {
		// String col_name = md.getColumnName(i);
		// String value = "";
		// if (col_name.equalsIgnoreCase("tpic")) {
		// // Get as a bytes.
		// byte[] bytes = rs.getBytes(i);
		// value = bytes.toString();
		// if (testMode) {
		// // Dump to file.
		// try {
		// FileOutputStream fos = new FileOutputStream(profileOwnerID + "-" +
		// cnt + "-thumbimage.bmp");
		// fos.write(bytes);
		// fos.close();
		// } catch(Exception ex) {
		// }
		// }
		// } else {
		// value = rs.getString(col_name);
		// if (col_name.equalsIgnoreCase("userid")) {
		// uid = value;
		// col_name = "userid";
		// }
		// }
		// values.put(col_name, new ObjectByteIterator(value.getBytes()));
		// }
		// // Fetch the thumbnail image from the file system.
		// if (insertImage && !FSimagePath.equals("") ){
		// byte[] thumbImage = GetImageFromFS(uid, false);
		// // Get the thumbnail image from the file.
		// if (testMode) {
		// // Dump to file.
		// try {
		// FileOutputStream fos = new FileOutputStream(profileOwnerID + "-" +
		// cnt + "-thumbimage.bmp");
		// fos.write(thumbImage);
		// fos.close();
		// } catch(Exception ex) {
		// }
		// }
		// values.put("tpic", new ObjectByteIterator(thumbImage));
		// }
		// results.add(values);
		// }
		// } catch (SQLException sx) {
		// retVal = ERROR;
		// sx.printStackTrace(System.out);
		// } finally {
		// try {
		// if (rs != null)
		// rs.close();
		// if(preparedStatement != null)
		// preparedStatement.clearParameters();
		// } catch (SQLException e) {
		// retVal = ERROR;
		// e.printStackTrace(System.out);
		// }
		// }

		return retVal;
	}
	
	public static String friendKey(int id1, int id2) {
		return Integer.toString(id1) + " " + Integer.toString(id2);
	}
	
	@Override
	public int acceptFriend(int inviterID, int inviteeID) {
		int retVal = SUCCESS;

//		Transaction tx = db.graphDb.beginTx();
//
//		
//		// find the current friendship relationship between these two nodes
//		
//		IndexHits<Relationship> result = db.friendshipIndex.get("ids", friendKey(inviterID, inviteeID));
//		if (result.size() == 0) {
//			CreateFriendship(inviterID, inviteeID);
//		} else if (result.size() == 1) {
//			try {
//				for (Relationship r : result) {
//					r.setProperty("status", "2");
//				}
//				tx.success();
//			} catch (Exception e) {
//				tx.failure();
//			} finally {
//				tx.finish();
//			}
//		} else {
//			System.err
//					.println("Error: in Neo4jDBClient.acceptFriend(...), friendship primary key violation. Exiting...");
//			System.exit(-4);
//		}
//		

		return retVal;
	}

	@Override
	public int rejectFriend(int inviterID, int inviteeID) {
		int retVal = SUCCESS;

/*
		ExecutionEngine engine = new ExecutionEngine(db.graphDb);
		ExecutionResult result = engine
				.execute("START n=node(*) MATCH (n) -[FRIENDSHIP]->(m) "
						+ "WHERE HAS(FRIENDSHIP.status) AND HAS(n.userid) AND HAS(m.userid) AND (FRIENDSHIP.status='1') AND "
						+ "(n.userid='" + inviterID + "') AND (m.userid='"
						+ inviteeID + "') DELETE FRIENDSHIP");
						*/

		Transaction tx = db.graphDb.beginTx();
		try {
			IndexHits<Relationship> result = db.friendshipIndex.get(
					"ids",
					Integer.toString(inviterID) + " "
							+ Integer.toString(inviteeID));

			if (result.size() == 0) {
				return retVal;
			} else if (result.size() == 1) {
				result.getSingle().delete();
			} else {
				System.err
						.println("Error: in Neo4jDBClient.acceptFriend(...), friendship primary key violation. Exiting...");
				System.exit(-4);
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
	public int inviteFriend(int inviterID, int inviteeID) {
		int retVal = SUCCESS;
		
		if (inviterID==inviteeID) {
			return retVal;
		}
		
		// find the current friendship relationship between these two nodes
		Transaction tx = db.graphDb.beginTx();
		try {
			IndexHits<Relationship> result = db.friendshipIndex.get(
					"ids",
					friendKey(inviterID,inviteeID));
			
			IndexHits<Relationship> result2 = db.friendshipIndex.get(
					"ids",
					friendKey(inviteeID,inviterID));
			
			
			if (result.size() == 0 && result2.size() == 0) {
				Node inviter = db.nodeIndex.get("userid",
						Integer.toString(inviterID)).getSingle();
				Node invitee = db.nodeIndex.get("userid",
						Integer.toString(inviteeID)).getSingle();
				Relationship f = inviter.createRelationshipTo(invitee,
						RelTypes.FRIENDSHIP);
				f.setProperty("status", "1");
				db.friendshipIndex.add(f, "ids", friendKey(inviterID, inviteeID));
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
		// ResultSet rs = null;
		//
		// if (profileOwnerID < 0 || requesterID < 0 || k < 0)
		// return ERROR;
		//
		// String query =
		// "SELECT * FROM resources WHERE walluserid = ? ORDER BY rid DESC LIMIT ?";
		// try {
		// if ((preparedStatement = newCachedStatements.get(GETTOPRES_STMT)) ==
		// null)
		// preparedStatement = createAndCacheStatement(GETTOPRES_STMT, query);
		// preparedStatement.setInt(1, profileOwnerID);
		// preparedStatement.setInt(2, (k+1));
		// rs = preparedStatement.executeQuery();
		// while (rs.next()) {
		// HashMap<String, ByteIterator> values = new HashMap<String,
		// ByteIterator>();
		// // Get the number of columns and their names.
		// ResultSetMetaData md = rs.getMetaData();
		// int col = md.getColumnCount();
		// for (int i = 1; i <= col; i++) {
		// String col_name = md.getColumnName(i);
		// String value = rs.getString(col_name);
		// if(col_name.equalsIgnoreCase("rid"))
		// col_name = "rid";
		// else if(col_name.equalsIgnoreCase("walluserid"))
		// col_name = "walluserid";
		// values.put(col_name, new ObjectByteIterator(value.getBytes()));
		// }
		// result.add(values);
		// }
		// } catch (SQLException sx) {
		// retVal = ERROR;
		// sx.printStackTrace(System.out);
		// } finally {
		// try {
		// if (rs != null)
		// rs.close();
		// if(preparedStatement != null)
		// preparedStatement.clearParameters();
		// } catch (SQLException e) {
		// retVal = ERROR;
		// e.printStackTrace(System.out);
		// }
		// }

		return retVal;
	}

	@Override
	public int getCreatedResources(int creatorID,
			Vector<HashMap<String, ByteIterator>> result) {
		int retVal = SUCCESS;
		IndexHits<Relationship> ret_resources = db.resourcesIndex.get("creatorid", Integer.toString(creatorID));
		
		HashMap<String, ByteIterator> m = new HashMap<String, ByteIterator>();
		for (Relationship r:ret_resources) {
			m.clear();
			//Iterator<String> itr = r.getPropertyKeys().iterator();			
			m.put("rid", new ObjectByteIterator(r.getProperty("rid").toString().getBytes())) ;
			result.add(m);
		}
		
		return retVal;
	}

	@Override
	public int viewCommentOnResource(int requesterID, int profileOwnerID,
			int resourceID, Vector<HashMap<String, ByteIterator>> result) {
		int retVal = SUCCESS;
		// ResultSet rs = null;
		//
		// if (profileOwnerID < 0 || requesterID < 0 || resourceID < 0)
		// return ERROR;
		//
		// String query = "";
		//
		// // Get comment count.
		// try {
		// query = "SELECT * FROM manipulation WHERE rid = ?";
		// if ((preparedStatement = newCachedStatements.get(GETRESCMT_STMT)) ==
		// null)
		// preparedStatement = createAndCacheStatement(GETRESCMT_STMT, query);
		// preparedStatement.setInt(1, resourceID);
		// rs = preparedStatement.executeQuery();
		// while (rs.next()) {
		// HashMap<String, ByteIterator> values = new HashMap<String,
		// ByteIterator>();
		// // Get the number of columns and their names.
		// ResultSetMetaData md = rs.getMetaData();
		// int col = md.getColumnCount();
		// for (int i = 1; i <= col; i++) {
		// String col_name = md.getColumnName(i);
		// String value = rs.getString(col_name);
		// values.put(col_name, new ObjectByteIterator(value.getBytes()));
		// }
		// result.add(values);
		// }
		// } catch(SQLException sx) {
		// retVal = ERROR;
		// sx.printStackTrace(System.out);
		// } finally {
		// try {
		// if (rs != null)
		// rs.close();
		// if(preparedStatement != null)
		// preparedStatement.clearParameters();
		// } catch (SQLException e) {
		// retVal = ERROR;
		// e.printStackTrace(System.out);
		// }
		// }

		return retVal;
	}

	@Override
	public int postCommentOnResource(int commentCreatorID, int profileOwnerID,
			int resourceID, HashMap<String, ByteIterator> values) {
		int retVal = SUCCESS;

		// if (profileOwnerID < 0 || commentCreatorID < 0 || resourceID < 0)
		// return ERROR;
		//
		// String query =
		// "INSERT INTO manipulation (mid, creatorid, rid, modifierid, timestamp, type, content) VALUES (?, ?, ?, ?, ?, ?, ?)";
		// try {
		// if ((preparedStatement = newCachedStatements.get(POSTCMT_STMT)) ==
		// null)
		// preparedStatement = createAndCacheStatement(POSTCMT_STMT, query);
		// preparedStatement.setInt(1,
		// Integer.parseInt(values.get("mid").toString()));
		// preparedStatement.setInt(2, profileOwnerID);
		// preparedStatement.setInt(3, resourceID);
		// preparedStatement.setInt(4, commentCreatorID);
		// preparedStatement.setString(5, values.get("timestamp").toString());
		// preparedStatement.setString(6, values.get("type").toString());
		// preparedStatement.setString(7, values.get("content").toString());
		// preparedStatement.executeUpdate();
		// } catch (SQLException sx) {
		// retVal = ERROR;
		// sx.printStackTrace(System.out);
		// } finally {
		// try {
		// if(preparedStatement != null)
		// preparedStatement.clearParameters();
		// } catch (SQLException e) {
		// retVal = ERROR;
		// e.printStackTrace(System.out);
		// }
		// }

		return retVal;
	}

	@Override
	public int delCommentOnResource(int resourceCreatorID, int resourceID,
			int manipulationID) {
		int retVal = SUCCESS;
		//
		// if (resourceCreatorID < 0 || resourceID < 0 || manipulationID < 0)
		// return ERROR;
		//
		// String query = "DELETE FROM manipulation WHERE mid = ? AND rid = ?";
		// try {
		// if ((preparedStatement = newCachedStatements.get(DELCMT_STMT)) ==
		// null)
		// preparedStatement = createAndCacheStatement(DELCMT_STMT, query);
		// preparedStatement.setInt(1, manipulationID);
		// preparedStatement.setInt(2, resourceID);
		// preparedStatement.executeUpdate();
		// } catch (SQLException sx) {
		// retVal = ERROR;
		// sx.printStackTrace(System.out);
		// } finally {
		// try {
		// if(preparedStatement != null)
		// preparedStatement.clearParameters();
		// } catch (SQLException e) {
		// retVal = ERROR;
		// e.printStackTrace(System.out);
		// }
		// }

		return retVal;
	}

	@Override
	public int thawFriendship(int friendid1, int friendid2) {
		int retVal = SUCCESS;

		//Remove index first
		//db.friendshipIndex.remove(db.friendshipIndex.get("ids", friendshipKey(friendid1, friendid2) ).getSingle());
		
		//ExecutionEngine engine = new ExecutionEngine(db.graphDb);
		//ExecutionResult result = engine
		//		.execute("START n=node(*) MATCH (n) -[FRIENDSHIP]-(m) "
		//				+ "WHERE HAS(n.userid) AND HAS(m.userid) AND HAS(FRIENDSHIP.status) "
		//				+ "AND (FRIENDSHIP.status='2') AND " + "(n.userid='"
		//				+ friendid1 + "') AND (m.userid='" + friendid2
		//				+ "') DELETE FRIENDSHIP");
		
		//db.friendshipIndex.get("ids", friendshipKey(friendid1, friendid2) ).getSingle()
		
		// if (friendid1 < 0 || friendid2 < 0)
		// return ERROR;
		//
		// String query =
		// "DELETE FROM friendship WHERE (inviterid = ? and inviteeid = ?) OR (inviterid = ? and inviteeid = ?) AND status = 2";
		// try {
		// if ((preparedStatement = newCachedStatements.get(UNFRNDFRND_STMT)) ==
		// null)
		// preparedStatement = createAndCacheStatement(UNFRNDFRND_STMT, query);
		// preparedStatement.setInt(1, friendid1);
		// preparedStatement.setInt(2, friendid2);
		// preparedStatement.setInt(3, friendid2);
		// preparedStatement.setInt(4, friendid1);
		//
		// preparedStatement.executeUpdate();
		// } catch(SQLException sx) {
		// retVal = ERROR;
		// sx.printStackTrace(System.out);
		// } finally {
		// try {
		// if(preparedStatement != null)
		// preparedStatement.clearParameters();
		// } catch (SQLException e) {
		// retVal = ERROR;
		// e.printStackTrace(System.out);
		// }
		// }

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
				// System.out.println("11111111111111111111111111111111111111111 "
				// + avgfriendsperuser);
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
				// System.out.println("11111111111111111111111111111111111111111 "
				// + avgpendingperuser);
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
			// HashMap<String, String>
			// Relationship resource_edge = db.createRelation(inviter, invitee,
			// propertyToSet);
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

		// try {
		// stmt = conn.createStatement();
		//
		// dropIndex(stmt, "RESOURCES_CREATORID");
		// dropIndex(stmt, "RESOURCES_WALLUSERID");
		// dropIndex(stmt, "FRIENDSHIP_INVITEEID");
		// dropIndex(stmt, "FRIENDSHIP_INVITERID");
		// dropIndex(stmt, "FRIENDSHIP_STATUS");
		// dropIndex(stmt, "MANIPULATION_RID");
		// dropIndex(stmt, "MANIPULATION_CREATORID");
		//
		// dropTable(stmt, "FRIENDSHIP");
		// dropTable(stmt, "MANIPULATION");
		// dropTable(stmt, "RESOURCES");
		// dropTable(stmt, "USERS");
		//
		// dropSequence(stmt, "MID_AUTO");
		//
		// stmt.executeUpdate("CREATE TABLE FRIENDSHIP"
		// + "(INVITERID INTEGER, INVITEEID INTEGER,"
		// + "STATUS INTEGER DEFAULT 1)");
		//
		// stmt.executeUpdate("CREATE TABLE MANIPULATION"
		// + "(MID INTEGER," + "CREATORID INTEGER, RID INTEGER,"
		// + "MODIFIERID INTEGER, TIMESTAMP VARCHAR(200),"
		// + "TYPE VARCHAR(200), CONTENT VARCHAR(200))");
		//
		// stmt.executeUpdate("CREATE TABLE RESOURCES"
		// + "(RID INTEGER,CREATORID INTEGER,"
		// + "WALLUSERID INTEGER, TYPE VARCHAR(200),"
		// + "BODY VARCHAR(200), DOC VARCHAR(200))");
		//
		// if
		// (Boolean.parseBoolean(props.getProperty(Client.INSERT_IMAGE_PROPERTY,
		// Client.INSERT_IMAGE_PROPERTY_DEFAULT))) {
		// stmt.executeUpdate("CREATE TABLE USERS"
		// + "(USERID INTEGER, USERNAME VARCHAR(200), "
		// + "PW VARCHAR(200), FNAME VARCHAR(200), "
		// + "LNAME VARCHAR(200), GENDER VARCHAR(200),"
		// + "DOB VARCHAR(200),JDATE VARCHAR(200), "
		// + "LDATE VARCHAR(200), ADDRESS VARCHAR(200),"
		// + "EMAIL VARCHAR(200), TEL VARCHAR(200), PIC BYTEA, TPIC BYTEA)");
		// } else {
		// stmt.executeUpdate("CREATE TABLE USERS"
		// + "(USERID INTEGER, USERNAME VARCHAR(200), "
		// + "PW VARCHAR(200), FNAME VARCHAR(200), "
		// + "LNAME VARCHAR(200), GENDER VARCHAR(200),"
		// + "DOB VARCHAR(200),JDATE VARCHAR(200), "
		// + "LDATE VARCHAR(200), ADDRESS VARCHAR(200),"
		// + "EMAIL VARCHAR(200), TEL VARCHAR(200))");
		// }
		//
		// /** Auto increment. */
		// stmt.executeUpdate("CREATE SEQUENCE MID_AUTO");
		// stmt.executeUpdate("ALTER TABLE MANIPULATION ALTER COLUMN MID SET DEFAULT NEXTVAL('MID_AUTO')");
		//
		// stmt.executeUpdate("ALTER TABLE USERS ALTER USERID SET NOT NULL");
		// stmt.executeUpdate("ALTER TABLE USERS ADD PRIMARY KEY (USERID)");
		// stmt.executeUpdate("ALTER TABLE MANIPULATION ADD PRIMARY KEY (MID)");
		// stmt.executeUpdate("ALTER TABLE MANIPULATION ALTER MID SET NOT NULL");
		// stmt.executeUpdate("ALTER TABLE MANIPULATION ALTER CREATORID SET NOT NULL");
		// stmt.executeUpdate("ALTER TABLE MANIPULATION ALTER RID SET NOT NULL");
		// stmt.executeUpdate("ALTER TABLE MANIPULATION ALTER MODIFIERID SET NOT NULL");
		// stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD PRIMARY KEY (INVITERID, INVITEEID)");
		// stmt.executeUpdate("ALTER TABLE FRIENDSHIP ALTER INVITERID SET NOT NULL");
		// stmt.executeUpdate("ALTER TABLE FRIENDSHIP ALTER INVITEEID SET NOT NULL");
		// stmt.executeUpdate("ALTER TABLE RESOURCES ADD PRIMARY KEY (RID)");
		// stmt.executeUpdate("ALTER TABLE RESOURCES ALTER RID SET NOT NULL");
		// stmt.executeUpdate("ALTER TABLE RESOURCES ALTER CREATORID SET NOT NULL");
		// stmt.executeUpdate("ALTER TABLE RESOURCES ALTER WALLUSERID SET NOT NULL");
		// stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_USERS_FK1 FOREIGN KEY (INVITERID)"
		// + "REFERENCES USERS (USERID) ON DELETE CASCADE");
		// stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_USERS_FK2 FOREIGN KEY (INVITEEID)"
		// + "REFERENCES USERS (USERID) ON DELETE CASCADE");
		// stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_RESOURCES_FK1 FOREIGN KEY (RID)"
		// + "REFERENCES RESOURCES (RID) ON DELETE CASCADE");
		// stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_USERS_FK1 FOREIGN KEY (CREATORID)"
		// + "REFERENCES USERS (USERID) ON DELETE CASCADE");
		// stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_USERS_FK2 FOREIGN KEY (MODIFIERID)"
		// + "REFERENCES USERS (USERID) ON DELETE CASCADE");
		// stmt.executeUpdate("ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_USERS_FK1 FOREIGN KEY (CREATORID)"
		// + "REFERENCES USERS (USERID) ON DELETE CASCADE");
		// stmt.executeUpdate("ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_USERS_FK2 FOREIGN KEY (WALLUSERID)"
		// + "REFERENCES USERS (USERID) ON DELETE CASCADE");
		//
		// buildIndexes(null);
		// } catch (SQLException e) {
		// e.printStackTrace(System.out);
		// } finally {
		// if (stmt != null)
		// try {
		// stmt.close();
		// } catch (SQLException e) {
		// e.printStackTrace(System.out);
		// }
		// }
	}

	@Override
	public void buildIndexes(Properties props) {
		// Statement stmt = null;
		// try {
		// stmt = conn.createStatement();
		// long startIdx = System.currentTimeMillis();
		//
		// stmt.executeUpdate("CREATE INDEX RESOURCES_CREATORID ON RESOURCES (CREATORID)");
		// stmt.executeUpdate("CREATE INDEX RESOURCES_WALLUSERID ON RESOURCES (WALLUSERID)");
		// stmt.executeUpdate("CREATE INDEX FRIENDSHIP_INVITEEID ON FRIENDSHIP (INVITEEID)");
		// stmt.executeUpdate("CREATE INDEX FRIENDSHIP_INVITERID ON FRIENDSHIP (INVITERID)");
		// stmt.executeUpdate("CREATE INDEX FRIENDSHIP_STATUS ON FRIENDSHIP (STATUS)");
		// stmt.executeUpdate("CREATE INDEX MANIPULATION_RID ON MANIPULATION (RID)");
		// stmt.executeUpdate("CREATE INDEX MANIPULATION_CREATORID ON MANIPULATION (CREATORID)");
		//
		// long endIdx = System.currentTimeMillis();
		// System.out.println("Time to build database index structures(ms):" +
		// (endIdx - startIdx));
		// } catch (Exception e) {
		// e.printStackTrace(System.out);
		// } finally {
		// try {
		// if (stmt != null)
		// stmt.close();
		// } catch (SQLException e) {
		// e.printStackTrace(System.out);
		// }
		// }
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
