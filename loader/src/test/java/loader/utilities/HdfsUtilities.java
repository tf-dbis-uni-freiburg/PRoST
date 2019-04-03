package loader.utilities;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Utilities for manipulation of files within the HDFS (only for test scope)
 *
 * @author Victor Anthony Arrascue Ayala
 */
public class HdfsUtilities {
	/**
	 * This method puts the file in an HDFS folder, so that this can be used for
	 * applications working on top of Hadoop. In case the HDFS folder passed as
	 * argument exists, this will be deleted.
	 *
	 * @param localPath
	 * @param hdfsFolderPath
	 * @throws IOException
	 */
	public static void putFileToHDFS(final String localPath, final String hdfsFolderPath, final JavaSparkContext jsc) throws IOException {
		// TODO: improve. Using the Hadoop API didn't work (see commented code).
		// fs.copyFromLocalFile(new
		// Path(triplesMoreThan3Resources.getAbsolutePath()), new
		// Path("/triplesMoreThanThreeElements"));
		// FileUtil.copy(fs, new
		// Path(triplesMoreThan3Resources.getAbsolutePath()), fs, new
		// Path("/triplesMoreThanThreeElements"), false,
		// sc.hadoopConfiguration());

		final FileSystem fs = FileSystem.get(jsc.hadoopConfiguration());
		if (fs.exists(new Path(hdfsFolderPath))) {
			fs.delete(new Path(hdfsFolderPath), true);
		}

		final JavaRDD<String> lines = jsc.textFile(localPath);
		lines.map(x -> x.replace("[", "").replace("]", "")).saveAsTextFile(hdfsFolderPath);
	}

}
