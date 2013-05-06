

package neo4j;

import org.neo4j.graphdb.RelationshipType;

// START SNIPPET: createRelTypes
public enum RelTypes implements RelationshipType {
	RESOURCE, FRIENDSHIP
}