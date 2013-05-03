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

package mongoDB;

public interface MongoDBClientConstants {
	public static final String MONGODB_URL_PROPERTY = "mongodb.url";
	public static final String MONGODB_DB_PROPERTY = "mongodb.database";
	public static final String MONGODB_WRITE_CONCERN_PROPERTY = "mongodb.writeConcern";
	public static final String MONGODB_SHARDING_PROPERTY = "sharding";
	public static final String MONGODB_SHARDING_PROPERTY_DEFAULT = "false";
	public static final String MONGODB_MANIPULATION_ARRAY_PROPERTY = "manipulationarray";
	public static final String MONGODB_MANIPULATION_ARRAY_PROPERTY_DEFAULT= "false";
	public static final String MONGODB_FRNDLIST_REQ_PROPERTY = "friendlistreq";
	public static final String MONGODB_FRNDLIST_REQ_PROPERTY_DEFAULT = "false";

}
