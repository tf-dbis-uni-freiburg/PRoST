package joinTree;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;
import executor.Utils;
import extVp.DatabaseStatistics;
import extVp.ExtVpCreator;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.util.Collections;
import java.util.List;

public class VpJoinNode extends Node {
	private static final Logger logger = Logger.getLogger("PRoST");
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

		//TODO remove constants from triplePatterns before creating creating VP Nodes. (literals are still kept in tripleGroup to be
		// executed after saving the extVPTable
		// literals might be the same, then must use the same var name in both vp nodes

		TriplePattern tempPattern1 = new TriplePattern(triplePattern1.triple, prefixes);
		TriplePattern tempPattern2 = new TriplePattern(triplePattern2.triple, prefixes);


		//TODO make sure that temp var names are unique to the pattern
		//convert constants to variables
		if (tempPattern1.subjectType != ElementType.VARIABLE){
			tempPattern1.subject = "?s1";
			tempPattern1.subjectType = ElementType.VARIABLE;
		}
		if (tempPattern1.objectType != ElementType.VARIABLE){
			tempPattern1.object = "?o1";
			tempPattern1.objectType = ElementType.VARIABLE;
		}

		if (tempPattern2.subjectType != ElementType.VARIABLE){
			tempPattern2.subject = "?s2";
			tempPattern2.subjectType = ElementType.VARIABLE;
		}
		if (tempPattern2.objectType != ElementType.VARIABLE){
			tempPattern2.object = "?o2";
			tempPattern2.objectType = ElementType.VARIABLE;
		}


		vpNode1 = new VpNode(tempPattern1, tableName1);
		vpNode2 = new VpNode(tempPattern2, tableName2);

		//logger.info("created VPJoinNode with patterns: " + tempPattern1.toString() + " " + tempPattern2.toString());

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


		//logger.info("computed vpjoin node: " + vpNode1.toString() + " " + vpNode2.toString());

		ExtVpCreator.createExtVpTablesFromJoinedVPs(sparkNodeData, spark, vpNode1.triplePattern.subject, vpNode1.triplePattern.object,
				vpNode2.triplePattern.subject, vpNode2.triplePattern.object, vpNode1.triplePattern.triple.getPredicate().toString(prefixes),
				vpNode2.triplePattern.triple.getPredicate().toString(prefixes), databaseStatistics, extVpDatabaseName, vp1Data.count(), vp2Data.count());
		
		//Apply constants and remove temporary variables
		if (tripleGroup.get(0).subjectType==ElementType.CONSTANT){
			//logger.info("applying constant s1:" + tripleGroup.get(0).subject);
			sparkNodeData = sparkNodeData.where("s1 = '" + tripleGroup.get(0).subject + "'").drop("s1");
		}
		if (tripleGroup.get(0).objectType==ElementType.CONSTANT){
			//logger.info("applying constant o1" + tripleGroup.get(0).object);
			sparkNodeData = sparkNodeData.where("o1 = '" + tripleGroup.get(0).object + "'").drop("o1");
		}
		if (tripleGroup.get(1).subjectType==ElementType.CONSTANT){
			//logger.info("applying constant s2" + tripleGroup.get(1).subject);
			sparkNodeData = sparkNodeData.where("s2 = '" + tripleGroup.get(1).subject + "'").drop("s2");
		}
		if (tripleGroup.get(1).objectType==ElementType.CONSTANT){
			//logger.info("applying constant o2" + tripleGroup.get(1).object);
			sparkNodeData = sparkNodeData.where("o2 = '" + tripleGroup.get(1).object + "'").drop("o2");
		}

	}
}