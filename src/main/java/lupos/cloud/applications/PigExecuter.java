package lupos.cloud.applications;

import java.io.IOException;
import java.util.Iterator;

import lupos.cloud.hbase.HBaseConnection;
import lupos.engine.evaluators.QueryEvaluator;
import lupos.misc.FileHelper;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;

public class PigExecuter {

	private static int times = 3;

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(final String[] args) throws IOException {

		HBaseConnection.init();
		final PigServer pigServer = new PigServer(ExecType.MAPREDUCE);

		for (int i = 0; i < args.length; i++) {

			System.out.println("Executing Pig Query: " + args[i]);
			final String pigQuery = FileHelper.fastReadFile(args[i]);

			final long[] exec = new long[PigExecuter.times];
			long execSum = 0;

			for(int j=0; j<PigExecuter.times; j++){
				final long start = System.currentTimeMillis();
				pigServer.registerQuery(pigQuery);
				final Iterator<Tuple> pigQueryResult = pigServer.openIterator("X");
				while(pigQueryResult.hasNext()){
					pigQueryResult.next();
				}
				final long end = System.currentTimeMillis();

				exec[j] = (end-start);
				execSum += exec[j];
			}

			System.out.println("Time in seconds: " + (execSum/1000.0));
			System.out.println("Standard deviation of the sample: " + QueryEvaluator.computeStandardDeviationOfTheSample(exec)/1000.0);
			System.out.println("Sample standard deviation: " + QueryEvaluator.computeSampleStandardDeviation(exec)/1000.0);
		}

		pigServer.shutdown();
	}

}