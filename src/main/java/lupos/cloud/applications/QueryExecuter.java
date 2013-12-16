/**
 * Copyright (c) 2013, Institute of Information Systems (Sven Groppe, Thomas Kiencke and contributors of P-LUPOSDATE), University of Luebeck
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
 * following conditions are met:
 *
 * 	- Redistributions of source code must retain the above copyright notice, this list of conditions and the following
 * 	  disclaimer.
 * 	- Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
 * 	  following disclaimer in the documentation and/or other materials provided with the distribution.
 * 	- Neither the name of the University of Luebeck nor the names of its contributors may be used to endorse or promote
 * 	  products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package lupos.cloud.applications;

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;

import org.apache.pig.impl.io.FileLocalizer;

import lupos.cloud.query.CloudEvaluator;
import lupos.cloud.storage.util.CloudManagement;
import lupos.datastructures.bindings.Bindings;
import lupos.datastructures.bindings.BindingsMap;
import lupos.datastructures.items.literal.LiteralFactory;
import lupos.datastructures.items.literal.URILiteral;
import lupos.datastructures.items.literal.LiteralFactory.MapType;
import lupos.datastructures.queryresult.QueryResult;
import lupos.engine.evaluators.BasicIndexQueryEvaluator;

/**
 * Dieser Executer führt beliebige SPARQL-Anfragen aus.
 */
public class QueryExecuter {

	private static CloudEvaluator cloudEvaluator;
	private static double[] result_time;
	private static double[] result_queryresult;
	private static double[] result_bitvectorTime;
	private static boolean printResults = true;
	static SimpleDateFormat formatter = new SimpleDateFormat(
			"yyyy.MM.dd HH:mm:ss");

	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			System.exit(0);
		}

		CloudManagement.PARALLEL_REDUCE_OPERATIONS = Integer.parseInt(args[0]);

		String testOnly = args[1];
		boolean printResults = true;

		if (args[2].equals("nosize")) {
			printResults = false;
		}

		cloudEvaluator = new CloudEvaluator();

		Bindings.instanceClass = BindingsMap.class;

		LiteralFactory.setType(MapType.NOCODEMAP);

		QueryResult.type = QueryResult.TYPE.ADAPTIVE;

		result_time = new double[args.length - 3];
		result_queryresult = new double[args.length - 3];
		result_bitvectorTime = new double[args.length - 3];

		// Tests ausführen:
		try {
			if (testOnly.equals("both") || testOnly.equals("first")) {
				System.out.println(formatter.format(new Date()).toString()
						+ ": Tests werden ausgefuehrt (mit bloomfilter):");

				for (int i = 0; i < args.length - 3; i++) {
					testQuery(i, args[i + 3], printResults);
					FileLocalizer.deleteTempFiles(); // loescht temp files auf
														// HDFS
				}
				printCSV();
			}

			if (testOnly.equals("both") || testOnly.equals("second")) {

				cloudEvaluator.getCloudManagement().bitvectorTime = 0.0;
				cloudEvaluator.getCloudManagement().bloomfilter_active = false;
				System.out.println(formatter.format(new Date()).toString()
						+ ": Tests werden ausgefuehrt (ohne bloomfilter):");
				for (int i = 0; i < args.length - 3; i++) {
					testQuery(i, args[i + 3], printResults);
					FileLocalizer.deleteTempFiles(); // loescht temp files auf
														// HDFS
				}
				printCSV();
			}

			cloudEvaluator.shutdown(); // gibt temporäre dateien frei
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void printCSV() {
		System.out.println("CSV Format: ");
		int i = 0;
		for (double result : result_time) {
			System.out.println(result + "\t" + result_bitvectorTime[i] + "\t"
					+ result_queryresult[i]);
			i++;
		}
	}

	protected static String readFile(String file) {
		InputStream is = QueryExecuter.class.getClassLoader()
				.getResourceAsStream(file);
		String selectQuery = convertStreamToString(is);
		return selectQuery;
	}

	private static String convertStreamToString(java.io.InputStream is) {
		java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
		return s.hasNext() ? s.next() : "";
	}

	protected static QueryResult executeQuery(
			BasicIndexQueryEvaluator evaluator, String query) throws Exception {
		evaluator.prepareInputData(new LinkedList<URILiteral>(),
				new LinkedList<URILiteral>()); // / workaround weil sonst
												// nullpointerexception
		evaluator.compileQuery(query);
		evaluator.logicalOptimization();
		evaluator.physicalOptimization();

		return evaluator.getResult(true);
	}

	public static void testQuery(int number, String filename, boolean printSize)
			throws Exception {
		System.out.println("\nTest " + number + " Input: " + filename);
		long start = System.currentTimeMillis();
		String selectQuery = readFile(filename);
		QueryResult actual = executeQuery(cloudEvaluator, selectQuery);
		long stop = System.currentTimeMillis();
		result_time[number] = (double) ((stop - start) / (double) 1000);
		System.out.print("Time: " + (double) ((stop - start) / (double) 1000)
				+ " Sekunden");
		if (printResults) {
			int resultSize = -1;
			if (printSize) {
				resultSize = actual.oneTimeSize();
			}
			result_queryresult[number] = resultSize;
			System.out.print("- Results: " + resultSize);
		}
		System.out.println();
		result_bitvectorTime[number] = cloudEvaluator.getCloudManagement()
				.getBitvectorTime();
	}
}
