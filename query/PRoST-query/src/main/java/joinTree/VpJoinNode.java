package joinTree;

import com.hp.hpl.jena.shared.PrefixMapping;
import executor.Utils;
import extVp.DatabaseStatistics;
import extVp.ExtVpCreator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.util.Collections;
import java.util.List;

public class VpJoinNode extends Node {
	private final VpNode vpNode1;
	private final VpNode vpNode2;

	private final SparkSession spark;
	private DatabaseStatistics databaseStatistics;
	private final String extVpDatabaseName;
	private final PrefixMapping prefixes;

	/*
	 * The node contains two childless VP Nodes, and after execution creates two semi join tables
	 */
	public VpJoinNode(final TriplePattern triplePattern1, final TriplePattern triplePattern2, final String tableName1,
					  final String tableName2, SparkSession spark, DatabaseStatistics databaseStatistics, final String extVpDatabaseName,
					  PrefixMapping prefixes) {
		super();

		tripleGroup.add(triplePattern1);
		tripleGroup.add(triplePattern2);

		//TODO remove literals from triplePatterns before creating creating VP Nodes. (literals are still kept in tripleGroup to be
		// executed after saving the extVPTable
		// literals might be the same, then must use the same var name in both vp nodes
		vpNode1 = new VpNode(triplePattern1, tableName1);
		vpNode2 = new VpNode(triplePattern2, tableName2);

		this.spark = spark;
		this.databaseStatistics = databaseStatistics;
		this.extVpDatabaseName = extVpDatabaseName;
		this.prefixes = prefixes;
	}

	@Override
	public void computeNodeData(final SQLContext sqlContext) {

		vpNode1.computeNodeData(sqlContext);
		vpNode2.computeNodeData(sqlContext);

		Dataset<Row> vp1Data = vpNode1.sparkNodeData;
		Dataset<Row> vp2Data = vpNode2.sparkNodeData;

		final List<String> joinVariables = Utils.commonVariables(vp1Data.columns(), vp2Data.columns());
		sparkNodeData = vp1Data.join(vp2Data, scala.collection.JavaConversions.asScalaBuffer(joinVariables).seq());

		ExtVpCreator.createExtVpTablesFromJoinedVPs(sparkNodeData, spark, vpNode1.triplePattern.subject, vpNode1.triplePattern.object,
				vpNode2.triplePattern.subject, vpNode2.triplePattern.object, vpNode1.triplePattern.triple.getPredicate().toString(prefixes),
				vpNode2.triplePattern.triple.getPredicate().toString(prefixes), databaseStatistics, extVpDatabaseName, vp1Data.count(), vp2Data.count());

		//TODO now would need to apply the literals to the data
		// sparkNodeData=

	}
}