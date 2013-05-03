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

package common;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.Client;
import edu.usc.bg.base.ObjectByteIterator;

public class RdbmsUtilities {

private static final boolean verbose = false;
	
	private static boolean StoreImageInFS(String userid, byte[] image, boolean profileimg, String FSimagePath){
		boolean result = true;
		String ext = "thumbnail";
		
		if (profileimg) ext = "profile";
		
		String ImageFileName = FSimagePath+"\\img"+userid+ext;
		
		File tgt = new File(ImageFileName);
		if ( tgt.exists() ){
			if (! tgt.delete() ) {
				System.out.println("Error, file exists and failed to delete");
				return false;
			}
		}

		//Write the file
		try{
			FileOutputStream fos = new FileOutputStream(ImageFileName);
			fos.write(image);
			fos.close();
		}catch(Exception ex){
			System.out.println("Error in writing the file"+ImageFileName);
			ex.printStackTrace(System.out);
		}

		return result;
	}
	
	private static byte[] GetImageFromFS(String userid, boolean profileimg, String FSimagePath){
        int filelength = 0;
        String ext = "thumbnail";
        byte[] imgpayload = null;
        BufferedInputStream bis = null;
       
        if (profileimg) ext = "profile";
       
        String ImageFileName = FSimagePath+"\\img"+userid+ext;
       int attempt = 100;
       while(attempt>0){
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
	             while (read < filelength && (numRead=dis.read(imgpayload, read, filelength - read    ) ) >= 0) {
	                     read = read + numRead;
	             }
	             dis.close();
	             fis.close();
	             break;
	         } catch (IOException e) {
	             e.printStackTrace(System.out);
	        	 attempt--;
	         }
       }
       return imgpayload;
	}
	
	public static int insertEntity(String entitySet, String entityPK, HashMap<String, ByteIterator> values, boolean insertImage, 
			Connection conn, String FSimagePath) {
		if (entitySet == null) {
			return -1;
		}
		if (entityPK == null) {
			return -1;
		}
		ResultSet rs =null;

		PreparedStatement preparedStatement = null;
		
		try {
			String query;
			int numFields = values.size();
			
			if(entitySet.equalsIgnoreCase("users") && insertImage && !FSimagePath.equals(""))
				numFields = numFields-2;
			query = "INSERT INTO "+entitySet+" VALUES (";
			for(int j=0; j<=numFields; j++){
				if(j==(numFields)){
					query+="?)";
					break;
				}else
					query+="?,";
			}

			preparedStatement = conn.prepareStatement(query);
			preparedStatement.setString(1, entityPK);
			int cnt=2;
			for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
				//blobs need to be inserted differently
				if(entry.getKey().equalsIgnoreCase("pic") || entry.getKey().equalsIgnoreCase("tpic") )
					continue;
				
				String field = entry.getValue().toString();
				preparedStatement.setString(cnt, field);
				cnt++;
				if (verbose) System.out.println(""+entry.getKey().toString()+":"+field);
			}

			if(entitySet.equalsIgnoreCase("users") && insertImage){
				byte[] profileImage = ((ObjectByteIterator)values.get("pic")).toArray();
				InputStream is = new ByteArrayInputStream(profileImage);
				if ( FSimagePath.equals("") )
					preparedStatement.setBinaryStream(numFields, is, profileImage.length);
				else
					StoreImageInFS(entityPK, profileImage, true, FSimagePath);
				
				byte[] thumbImage = ((ObjectByteIterator)values.get("tpic")).toArray();
				is = new ByteArrayInputStream(thumbImage);
				
				if (FSimagePath.equals(""))
					preparedStatement.setBinaryStream(numFields+1, is, thumbImage.length);
				else
					StoreImageInFS(entityPK, thumbImage, false, FSimagePath);
			}
			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			System.out.println("Error in processing insert to table: " + entitySet + e);
			return -2;
		}finally{
			try {
				if (rs != null)
					rs.close();
				if(preparedStatement != null)
					preparedStatement.close();
			} catch (SQLException e) {
				e.printStackTrace(System.out);
				return -1;
			}
		}
		return 0;
	}
	
	
	public static void createSchema(Properties props, Connection conn){

		Statement stmt = null;

		try {
			stmt = conn.createStatement();

			//dropSequence(stmt, "MIDINC");
			dropSequence(stmt, "RIDINC");
			dropSequence(stmt, "USERIDINC");
			dropSequence(stmt, "USERIDS");

			dropTable(stmt, "friendship");
			dropTable(stmt, "manipulation");
			dropTable(stmt, "resources");
			dropTable(stmt, "users");

			//stmt.executeUpdate("CREATE SEQUENCE  MIDINC  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 201 CACHE 20 NOORDER  NOCYCLE");
			stmt.executeUpdate("CREATE SEQUENCE  RIDINC  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 21 CACHE 20 NOORDER  NOCYCLE ");
			stmt.executeUpdate("CREATE SEQUENCE  USERIDINC  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 21 CACHE 20 NOORDER  NOCYCLE ");
			stmt.executeUpdate("CREATE SEQUENCE  USERIDS  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 1 CACHE 20 NOORDER  NOCYCLE");

			stmt.executeUpdate("CREATE TABLE FRIENDSHIP"
					+ "(INVITERID NUMBER, INVITEEID NUMBER,"
					+ "STATUS NUMBER DEFAULT 1" + ") NOLOGGING");

			stmt.executeUpdate("CREATE TABLE MANIPULATION"
					+ "(	MID NUMBER," + "CREATORID NUMBER, RID NUMBER,"
					+ "MODIFIERID NUMBER, TIMESTAMP VARCHAR2(200),"
					+ "TYPE VARCHAR2(200), CONTENT VARCHAR2(200)"
					+ ") NOLOGGING");

			stmt.executeUpdate("CREATE TABLE RESOURCES"
					+ "(	RID NUMBER,CREATORID NUMBER,"
					+ "WALLUSERID NUMBER, TYPE VARCHAR2(200),"
					+ "BODY VARCHAR2(200), DOC VARCHAR2(200)"
					+ ") NOLOGGING");

			if (Boolean.parseBoolean(props.getProperty(Client.INSERT_IMAGE_PROPERTY,
					Client.INSERT_IMAGE_PROPERTY_DEFAULT))) {
				stmt.executeUpdate("CREATE TABLE USERS"
						+ "(USERID NUMBER, USERNAME VARCHAR2(200), "
						+ "PW VARCHAR2(200), FNAME VARCHAR2(200), "
						+ "LNAME VARCHAR2(200), GENDER VARCHAR2(200),"
						+ "DOB VARCHAR2(200),JDATE VARCHAR2(200), "
						+ "LDATE VARCHAR2(200), ADDRESS VARCHAR2(200),"
						+ "EMAIL VARCHAR2(200), TEL VARCHAR2(200), PIC BLOB, TPIC BLOB"
						+ ") NOLOGGING");
			} else {
				stmt.executeUpdate("CREATE TABLE USERS"
						+ "(USERID NUMBER, USERNAME VARCHAR2(200), "
						+ "PW VARCHAR2(200), FNAME VARCHAR2(200), "
						+ "LNAME VARCHAR2(200), GENDER VARCHAR2(200),"
						+ "DOB VARCHAR2(200),JDATE VARCHAR2(200), "
						+ "LDATE VARCHAR2(200), ADDRESS VARCHAR2(200),"
						+ "EMAIL VARCHAR2(200), TEL VARCHAR2(200)"
						+ ") NOLOGGING");

			}

			stmt.executeUpdate("ALTER TABLE USERS MODIFY (USERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE USERS ADD CONSTRAINT USERS_PK PRIMARY KEY (USERID) ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_PK PRIMARY KEY (MID,RID) ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (MID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (CREATORID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (RID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (MODIFIERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_PK PRIMARY KEY (INVITERID, INVITEEID) ENABLE");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP MODIFY (INVITERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP MODIFY (INVITEEID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_PK PRIMARY KEY (RID) ENABLE");
			stmt.executeUpdate("ALTER TABLE RESOURCES MODIFY (RID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE RESOURCES MODIFY (CREATORID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE RESOURCES MODIFY (WALLUSERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_USERS_FK1 FOREIGN KEY (INVITERID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_USERS_FK2 FOREIGN KEY (INVITEEID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_RESOURCES_FK1 FOREIGN KEY (RID)"
					+ "REFERENCES RESOURCES (RID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_USERS_FK1 FOREIGN KEY (CREATORID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_USERS_FK2 FOREIGN KEY (MODIFIERID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_USERS_FK1 FOREIGN KEY (CREATORID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_USERS_FK2 FOREIGN KEY (WALLUSERID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
		

			stmt.executeUpdate("CREATE OR REPLACE TRIGGER RINC before insert on resources "
					+ "for each row "
					+ "WHEN (new.rid is null) begin "
					+ "select ridInc.nextval into :new.rid from dual;"
					+ "end;");
			stmt.executeUpdate("ALTER TRIGGER RINC ENABLE");

			stmt.executeUpdate("CREATE OR REPLACE TRIGGER UINC before insert on users "
					+ "for each row "
					+ "WHEN (new.userid is null) begin "
					+ "select useridInc.nextval into :new.userid from dual;"
					+ "end;");
			stmt.executeUpdate("ALTER TRIGGER UINC ENABLE");
			
			dropIndex(stmt, "RESOURCE_CREATORID");
			dropIndex(stmt, "RESOURCES_WALLUSERID");
			dropIndex(stmt, "FRIENDSHIP_INVITEEID");
			dropIndex(stmt, "FRIENDSHIP_STATUS");
			dropIndex(stmt, "FRIENDSHIP_INVITERID");
			dropIndex(stmt, "MANIPULATION_RID");
			dropIndex(stmt, "MANIPULATION_CREATORID");
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		} finally {
			if (stmt != null)
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace(System.out);
				}
		}
	}

	public static void buildIndexes(Properties props, Connection conn){
		Statement stmt  = null;
		try {
			stmt = conn.createStatement();
			long startIdx = System.currentTimeMillis();

			dropIndex(stmt, "RESOURCE_CREATORID");
			dropIndex(stmt, "RESOURCES_WALLUSERID");
			dropIndex(stmt, "FRIENDSHIP_STATUS");
			dropIndex(stmt, "FRIENDSHIP_INVITEEID");
			dropIndex(stmt, "FRIENDSHIP_INVITERID");
			dropIndex(stmt, "MANIPULATION_RID");
			dropIndex(stmt, "MANIPULATION_CREATORID");
			stmt.executeUpdate("CREATE INDEX RESOURCE_CREATORID ON RESOURCES (CREATORID)"
					+ "COMPUTE STATISTICS NOLOGGING");

			stmt.executeUpdate("CREATE INDEX FRIENDSHIP_INVITEEID ON FRIENDSHIP (INVITEEID)"
					+ "COMPUTE STATISTICS NOLOGGING");

			stmt.executeUpdate("CREATE INDEX MANIPULATION_RID ON MANIPULATION (RID)"
					+ "COMPUTE STATISTICS NOLOGGING");

			stmt.executeUpdate("CREATE INDEX RESOURCES_WALLUSERID ON RESOURCES (WALLUSERID)"
					+ "COMPUTE STATISTICS NOLOGGING");

			stmt.executeUpdate("CREATE INDEX FRIENDSHIP_INVITERID ON FRIENDSHIP (INVITERID)"
					+ "COMPUTE STATISTICS NOLOGGING");

			
			stmt.executeUpdate("CREATE INDEX FRIENDSHIP_STATUS ON FRIENDSHIP (STATUS)"
					+ "COMPUTE STATISTICS NOLOGGING");

			
			stmt.executeUpdate("CREATE INDEX MANIPULATION_CREATORID ON MANIPULATION (CREATORID)"
					+ "COMPUTE STATISTICS NOLOGGING");
			
			stmt.executeUpdate("analyze table users compute statistics");
			stmt.executeUpdate("analyze table resources compute statistics");
			stmt.executeUpdate("analyze table friendship compute statistics");
			stmt.executeUpdate("analyze table manipulation compute statistics");
			long endIdx = System.currentTimeMillis();
			System.out
			.println("Time to build database index structures(ms):"
					+ (endIdx - startIdx));
		} catch (Exception e) {
			e.printStackTrace(System.out);
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}
	}

	private static void dropSequence(Statement st, String seqName) {
		try {
			st.executeUpdate("drop sequence " + seqName);
		} catch (SQLException e) {
		}
	}

	private static void dropIndex(Statement st, String idxName) {
		try {
			st.executeUpdate("drop index " + idxName);
		} catch (SQLException e) {
		}
	}

	private static void dropTable(Statement st, String tableName) {
		try {
			st.executeUpdate("drop table " + tableName);
		} catch (SQLException e) {
		}
	}
	
	
	public static void createSchemaSqlServer(Properties props, Connection conn){

		Statement stmt = null;

		try {
			stmt = conn.createStatement();

			//dropSequence(stmt, "MIDINC");
			//dropSequence(stmt, "RIDINC");
			//dropSequence(stmt, "USERIDINC");
			//dropSequence(stmt, "USERIDS");

			dropTable(stmt, "friendship");
			dropTable(stmt, "manipulation");
			dropTable(stmt, "resources");
			dropTable(stmt, "users");

			//stmt.executeUpdate("CREATE SEQUENCE  MIDINC  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 201 CACHE 20 NOORDER  NOCYCLE");
			//stmt.executeUpdate("CREATE SEQUENCE  RIDINC  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 21 CACHE 20 NOORDER  NOCYCLE ");
			//stmt.executeUpdate("CREATE SEQUENCE  USERIDINC  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 21 CACHE 20 NOORDER  NOCYCLE ");
			//stmt.executeUpdate("CREATE SEQUENCE  USERIDS  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 1 CACHE 20 NOORDER  NOCYCLE");

			stmt.executeUpdate("CREATE TABLE FRIENDSHIP"
					+ "(INVITERID INT NOT NULL, INVITEEID INT NOT NULL,"
					+ "STATUS INT DEFAULT 1" + ")");

			stmt.executeUpdate("CREATE TABLE MANIPULATION"
					+ "(	MID INT NOT NULL," + "CREATORID INT NOT NULL, RID INT NOT NULL,"
					+ "MODIFIERID INT, TIMESTAMP VARCHAR(200),"
					+ "TYPE VARCHAR(200), CONTENT VARCHAR(200)"
					+ ")");

			stmt.executeUpdate("CREATE TABLE RESOURCES"
					+ "(	RID INT PRIMARY KEY, CREATORID INT NOT NULL,"
					+ "WALLUSERID INT, TYPE VARCHAR(200),"
					+ "BODY VARCHAR(200), DOC VARCHAR(200)"
					+ ")");

			if (Boolean.parseBoolean(props.getProperty(Client.INSERT_IMAGE_PROPERTY,
					Client.INSERT_IMAGE_PROPERTY_DEFAULT)) ) {
				stmt.executeUpdate("CREATE TABLE USERS"
						+ "(USERID INT PRIMARY KEY, USERNAME VARCHAR(200), "
						+ "PW VARCHAR(200), FNAME VARCHAR(200), "
						+ "LNAME VARCHAR(200), GENDER VARCHAR(200),"
						+ "DOB VARCHAR(200),JDATE VARCHAR(200), "
						+ "LDATE VARCHAR(200), ADDRESS VARCHAR(200),"
						+ "EMAIL VARCHAR(200), TEL VARCHAR(200), PIC image, TPIC image"
						+ ")");
			} else {
				stmt.executeUpdate("CREATE TABLE USERS"
						+ "(USERID INT PRIMARY KEY, USERNAME VARCHAR(200), "
						+ "PW VARCHAR(200), FNAME VARCHAR(200), "
						+ "LNAME VARCHAR(200), GENDER VARCHAR(200),"
						+ "DOB VARCHAR(200),JDATE VARCHAR(200), "
						+ "LDATE VARCHAR(200), ADDRESS VARCHAR(200),"
						+ "EMAIL VARCHAR(200), TEL VARCHAR(200)"
						+ ")");

			}

			//stmt.executeUpdate("ALTER TABLE USERS MODIFY (USERID NOT NULL ENABLE)");
			//stmt.executeUpdate("ALTER TABLE USERS ADD CONSTRAINT USERS_PK PRIMARY KEY (USERID)");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_PK PRIMARY KEY (MID,RID)");
			//stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (MID NOT NULL ENABLE)");
			//stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (CREATORID NOT NULL ENABLE)");
			//stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (RID NOT NULL ENABLE)");
			//stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (MODIFIERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_PK PRIMARY KEY (INVITERID, INVITEEID)");
			//stmt.executeUpdate("ALTER TABLE FRIENDSHIP MODIFY (INVITERID NOT NULL ENABLE)");
			//stmt.executeUpdate("ALTER TABLE FRIENDSHIP MODIFY (INVITEEID NOT NULL ENABLE)");
			//stmt.executeUpdate("ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_PK PRIMARY KEY (RID) ");
			//stmt.executeUpdate("ALTER TABLE RESOURCES MODIFY (RID NOT NULL ENABLE)");
			//stmt.executeUpdate("ALTER TABLE RESOURCES MODIFY (CREATORID NOT NULL ENABLE)");
			//stmt.executeUpdate("ALTER TABLE RESOURCES MODIFY (WALLUSERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_USERS_FK1 FOREIGN KEY (INVITERID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_USERS_FK2 FOREIGN KEY (INVITEEID)"
					+ "REFERENCES USERS (USERID) ON DELETE NO ACTION ");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_RESOURCES_FK1 FOREIGN KEY (RID)"
					+ "REFERENCES RESOURCES (RID) ON DELETE CASCADE ");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_USERS_FK1 FOREIGN KEY (CREATORID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_USERS_FK2 FOREIGN KEY (MODIFIERID)"
					+ "REFERENCES USERS (USERID) ON DELETE NO ACTION ");
			// TODO: make these two constraints work with SQL Server
//			stmt.executeUpdate("ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_USERS_FK1 FOREIGN KEY (CREATORID)"
//					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ");
//			stmt.executeUpdate("ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_USERS_FK2 FOREIGN KEY (WALLUSERID)"
//					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ");
		
//
//			stmt.executeUpdate("CREATE OR REPLACE TRIGGER RINC before insert on resources "
//					+ "for each row "
//					+ "WHEN (new.rid is null) begin "
//					+ "select ridInc.nextval into :new.rid from dual;"
//					+ "end;");
//			stmt.executeUpdate("ALTER TRIGGER RINC ENABLE");
//
//			stmt.executeUpdate("CREATE OR REPLACE TRIGGER UINC before insert on users "
//					+ "for each row "
//					+ "WHEN (new.userid is null) begin "
//					+ "select useridInc.nextval into :new.userid from dual;"
//					+ "end;");
//			stmt.executeUpdate("ALTER TRIGGER UINC ENABLE");
			
			dropIndex(stmt, "RESOURCE_CREATORID");
			dropIndex(stmt, "RESOURCES_WALLUSERID");
			dropIndex(stmt, "FRIENDSHIP_INVITEEID");
			dropIndex(stmt, "FRIENDSHIP_STATUS");
			dropIndex(stmt, "FRIENDSHIP_INVITERID");
			dropIndex(stmt, "MANIPULATION_RID");
			dropIndex(stmt, "MANIPULATION_CREATORID");
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		} finally {
			if (stmt != null)
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace(System.out);
				}
		}
	}

	public static void buildIndexesSqlServer(Properties props, Connection conn){
		Statement stmt  = null;
		try {
			stmt = conn.createStatement();
			long startIdx = System.currentTimeMillis();

			dropIndex(stmt, "RESOURCE_CREATORID");
			dropIndex(stmt, "RESOURCES_WALLUSERID");
			dropIndex(stmt, "FRIENDSHIP_STATUS");
			dropIndex(stmt, "FRIENDSHIP_INVITEEID");
			dropIndex(stmt, "FRIENDSHIP_INVITERID");
			dropIndex(stmt, "MANIPULATION_RID");
			dropIndex(stmt, "MANIPULATION_CREATORID");
			stmt.executeUpdate("CREATE INDEX RESOURCE_CREATORID ON RESOURCES (CREATORID)"
					+ " ");

			stmt.executeUpdate("CREATE INDEX FRIENDSHIP_INVITEEID ON FRIENDSHIP (INVITEEID)"
					+ " ");

			stmt.executeUpdate("CREATE INDEX MANIPULATION_RID ON MANIPULATION (RID)"
					+ " ");

			stmt.executeUpdate("CREATE INDEX RESOURCES_WALLUSERID ON RESOURCES (WALLUSERID)"
					+ " ");

			stmt.executeUpdate("CREATE INDEX FRIENDSHIP_INVITERID ON FRIENDSHIP (INVITERID)"
					+ " ");

			
			stmt.executeUpdate("CREATE INDEX FRIENDSHIP_STATUS ON FRIENDSHIP (STATUS)"
					+ " ");

			
			stmt.executeUpdate("CREATE INDEX MANIPULATION_CREATORID ON MANIPULATION (CREATORID)"
					+ " ");
			
			stmt.executeUpdate("CREATE STATISTICS users_stats ON users (userid)");
			stmt.executeUpdate("CREATE STATISTICS resources_stats ON resources (rid, CREATORID)");
			stmt.executeUpdate("CREATE STATISTICS friendship_stats ON friendship (inviterid, inviteeid, status)");
			stmt.executeUpdate("CREATE STATISTICS manipulation_stats ON manipulation (MID, CREATORID, RID, MODIFIERID)");
			long endIdx = System.currentTimeMillis();
			System.out
			.println("Time to build database index structures(ms):"
					+ (endIdx - startIdx));
		} catch (Exception e) {
			e.printStackTrace(System.out);
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}
	}
}
