package JoinTree;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;


/**
 * JoinTree Java definition
 * @author Matteo Cossu
 *
 */
public class JoinTree {
	
	private Node node;
	
	public Node getNode() {
		return node;
	}

	
	public JoinTree(Node node){
		this.node = node;
	}
	
	public void computeSingularNodeData(SQLContext sqlContext){
		node.computeSubTreeData(sqlContext);		
	}
	
	public Dataset<Row> computeJoins(SQLContext sqlContext){
		// compute all the joins
		Dataset<Row> results = node.computeJoinWithChildren(sqlContext);
		// select only the requested result
		Column [] selectedColumns = new Column[node.projection.size()];
		for (int i = 0; i < selectedColumns.length; i++) {
			selectedColumns[i]= new Column(node.projection.get(i));
		}

		return results.select(selectedColumns);
		
	}
	
	
		@Override
	public String toString(){
		StringBuilder str = new StringBuilder("JoinTree \n");
		str.append(node.toString());
		for (Node child: node.children){
			str.append("\n" + child.toString());
		}
		
		return str.toString();
	}

}
