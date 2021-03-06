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


package edu.usc.bg.workloads;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.Vector;

import edu.usc.bg.base.Client;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.Workload;
import edu.usc.bg.base.WorkloadException;

/**
 * Loads friendship for the users
 * @author barahman
 *
 */
public class FriendshipWorkload extends Workload {

	// The name of the table to insert records.
	public final static String table = "friendship";
	// The number of fields in a record.
	public final static int fieldCount = 2;
	// The length of each field for a record in Byte.
	public final static int fieldLength = 100;
	// The name of each field for the "friendship" table.
	public final static String[] fieldName = {"inviterid", "inviteeid", "status"};

	// These following fields could be kept in the property file.	
	// The percentage of pending friend.
	public static float friendPercentage = 0.01f;
	// The average number of friends per user.
	public static int avgFriendCount = 0;
	// The number of users.
	public int userCount = 100;
	// The number of records to be inserted.
	public int recordCount = userCount * avgFriendCount; // Friendship number.
		
	int flags[];
	Vector<Integer> _members;
	Random random = new Random();

	public FriendshipWorkload() {

	}

	// Initialize all of the threads with the same configuration.
	public void init(Properties p, Vector<Integer> members) throws WorkloadException {
		userCount=Integer.parseInt(p.getProperty(Client.USER_COUNT_PROPERTY));
		avgFriendCount=Integer.parseInt(p.getProperty(Client.FRIENDSHIP_COUNT_PROPERTY));
		friendPercentage=1-Float.parseFloat(p.getProperty(Client.CONFPERC_COUNT_PROPERTY));
		recordCount = userCount * avgFriendCount; // Friendship number.
		flags = new int[userCount];
		_members = members;
		for(int i=0; i<userCount; i++)
			flags[i] = 0;

	}

	// Prepare the record for "friendship" table.
	// inviterid, inviteeid, status.
	private void addFriends(DB db, int dbKey, int keyNum) {
		int res = 0;
		// Generate the fields using StringByteIterator and RandomByteIterator.
		if(friendPercentage*(random.nextInt(userCount)+1) >= 1.0f){		
			res = db.inviteFriend(dbKey, keyNum); //data = new StringByteIterator("1"); // Pending.
		}else {					
			res = db.CreateFriendship(dbKey, keyNum); //data = new StringByteIterator("2"); // Already.
		}
		
		if(res < 0){
			System.out.println("The creation of the friendship relationship failes. " +
					"Please make sure the appropriate schema has been created " + 
					" and the users have been inserted.");
			System.exit(-1);
		}

	}

	// Prepare the primary key.
	public String buildKeyName(long keyNum) {
		String keyNumStr = "" + keyNum;
		return keyNumStr;
	}

	@Override
	public boolean doInsert(DB db, Object threadState) {
		int dbKey = -1;
		int dbIdx = -1;
		for(int i=0; i<userCount; i++){
			if(flags[i] <avgFriendCount/2){
				dbKey = _members.get(i);
				dbIdx = i;
				break;
			}	
		}
		if(dbKey == -1 || dbIdx == -1){
			return true;
		}

		int friendid = -1;
		int friendIdx = -1;
		for(int i=0; i<(avgFriendCount/2); i++){
			friendIdx = (dbIdx+i+1) % userCount;
			
			friendid = _members.get(friendIdx);
			addFriends(db, dbKey,friendid);
			flags[dbIdx] = flags[dbIdx]+1;
		}
		return true;
	}

	@Override
	public HashMap<String, String> getDBInitialStats(DB db) {
		HashMap<String, String> stats = new HashMap<String, String>();
		stats = db.getInitialStats();
		return stats;
	}

	@Override
	public int doTransaction(DB db, Object threadstate, int threadid,
			StringBuilder updateLog, StringBuilder readLog, int seqID,
			HashMap<String, Integer> resUpdateOperations,
			HashMap<String, Integer> frienshipInfo,
			HashMap<String, Integer> pendingInfo, int thinkTime,
			boolean insertImage, boolean warmup) {
		return 0;
	}










}
