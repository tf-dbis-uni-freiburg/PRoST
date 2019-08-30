package translator;

import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import statistics.DatabaseStatistics;
import translator.algebraTree.Operation;
import utils.Settings;

/**
 * A SPARQL query object.
 */
public class Query {
	private static final Logger logger = Logger.getLogger("PRoST");
	private final String path;
	private final Operation algebraTree;
	private final Settings settings;

	public Query(final String queryPath,
				 final DatabaseStatistics statistics, final Settings settings) throws Exception {
		this.settings = settings;
		this.path = queryPath;

		final com.hp.hpl.jena.query.Query jenaQuery = QueryFactory.read("file:" + queryPath);
		final PrefixMapping prefixes = jenaQuery.getPrefixMapping();
		final String sparqlQuery = jenaQuery.toString();

		final Op jenaAlgebraTree = Algebra.compile(jenaQuery);
		logger.info("** COMPUTING PRoST ALGEBRA TREE for **\n" + sparqlQuery + "\n****************");
		this.algebraTree = new Operation.Builder(jenaAlgebraTree, statistics, settings, prefixes).build();
		logger.info("** PRoST ALGEBRA TREE **\n" + this.algebraTree + "\n****************");
	}

	public Dataset<Row> compute(final SQLContext sqlContext) {
		return algebraTree.computeOperation(sqlContext);
	}

	public Dataset<Row> compute() {
		// initialize the Spark environment
		final SparkSession spark = SparkSession.builder().appName("PRoST-Executor").enableHiveSupport().getOrCreate();
		final SQLContext sqlContext = spark.sqlContext();
		sqlContext.sql("USE " + settings.getDatabaseName());
		return algebraTree.computeOperation(sqlContext);
	}

	public String getPath() {
		return this.path;
	}

	public Operation getAlgebraTree() {
		return algebraTree;
	}
}
