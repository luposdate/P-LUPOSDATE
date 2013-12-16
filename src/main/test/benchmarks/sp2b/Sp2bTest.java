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
package benchmarks.sp2b;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;

import lupos.cloud.applications.HBaseLoader;
import lupos.cloud.hbase.HBaseConnection;
import lupos.cloud.query.CloudEvaluator;
import lupos.datastructures.items.literal.URILiteral;
import lupos.datastructures.queryresult.QueryResult;
import lupos.engine.evaluators.BasicIndexQueryEvaluator;

import org.junit.BeforeClass;

public class Sp2bTest {

	protected static final String file_100_triples = "benchmarks/sp2b/sp2b_100.n3";
	protected static final String file_1000_triples = "benchmarks/sp2b/sp2b_1000.n3";
	protected static final String file_10000_triples = "benchmarks/sp2b/sp2b_10000.n3";
	protected static final String file_500_triples = "benchmarks/sp2b/sp2b_500.n3";
	protected static final String file_5000_triples = "benchmarks/sp2b/sp2b_5000.n3";
	protected static final String file_50000_triples = "benchmarks/sp2b/sp2b_50000.n3";
	protected static final String file_q1 = "benchmarks/sp2b/sp2b_q1.n3";
	protected static final String file_q2 = "benchmarks/sp2b/sp2b_q2.n3";
	protected static final String file_q2_with_optional = "benchmarks/sp2b/sp2b_q2_with_optional.n3";
	protected static final String file_q3a = "benchmarks/sp2b/sp2b_q3a.n3";
	protected static final String file_q3b = "benchmarks/sp2b/sp2b_q3b.n3";
	protected static final String file_q3c = "benchmarks/sp2b/sp2b_q3c.n3";
	protected static final String file_q4 = "benchmarks/sp2b/sp2b_q4.n3";
	protected static final String file_q5a = "benchmarks/sp2b/sp2b_q5a.n3";
	protected static final String file_q5b = "benchmarks/sp2b/sp2b_q5b.n3";
	protected static final String file_q6 = "benchmarks/sp2b/sp2b_q6.n3";
	protected static final String file_q7 = "benchmarks/sp2b/sp2b_q7.n3";
	protected static final String file_q7_with_optional = "benchmarks/sp2b/sp2b_q7_with_optional.n3";
	protected static final String file_q8 = "benchmarks/sp2b/sp2b_q8.n3";
	protected static final String file_q9 = "benchmarks/sp2b/sp2b_q9.n3";
	protected static final String file_q10 = "benchmarks/sp2b/sp2b_q10.n3";
	protected static final String file_q11 = "benchmarks/sp2b/sp2b_q11.n3";
	protected static final String file_q12a = "benchmarks/sp2b/sp2b_q12a.n3";
	protected static final String file_q12b = "benchmarks/sp2b/sp2b_q12b.n3";
	protected static final String file_q12c = "benchmarks/sp2b/sp2b_q12c.n3";
	protected static final String file_test = "benchmarks/sp2b/sp2b_test.n3";
	

	protected static final String q1_query_filename = "benchmarks/sp2b/queries/q1.sparql";
	protected static final String q2_query_filename = "benchmarks/sp2b/queries/q2.sparql";
	protected static final String q3a_query_filename = "benchmarks/sp2b/queries/q3a.sparql";
	protected static final String q3b_query_filename = "benchmarks/sp2b/queries/q3b.sparql";
	protected static final String q3c_query_filename = "benchmarks/sp2b/queries/q3c.sparql";
	protected static final String q4_query_filename = "benchmarks/sp2b/queries/q4.sparql";
	protected static final String q5a_query_filename = "benchmarks/sp2b/queries/q5a.sparql";
	protected static final String q5b_query_filename = "benchmarks/sp2b/queries/q5b.sparql";
	protected static final String q6_query_filename = "benchmarks/sp2b/queries/q6.sparql";
	protected static final String q7_query_filename = "benchmarks/sp2b/queries/q7.sparql";
	protected static final String q8_query_filename = "benchmarks/sp2b/queries/q8.sparql";
	protected static final String q9_query_filename = "benchmarks/sp2b/queries/q9.sparql";
	protected static final String q10_query_filename = "benchmarks/sp2b/queries/q10.sparql";
	protected static final String q11_query_filename = "benchmarks/sp2b/queries/q11.sparql";
	protected static final String q12a_query_filename = "benchmarks/sp2b/queries/q12a.sparql";
	protected static final String q12b_query_filename = "benchmarks/sp2b/queries/q12b.sparql";
	protected static final String q12c_query_filename = "benchmarks/sp2b/queries/q12c.sparql";
	
	protected static final String default_file = file_10000_triples;

	protected static CloudEvaluator cloudEvaluator;

	@BeforeClass
	public static void initCloud() throws Exception {
		cloudEvaluator = initCloudEvaluator();

	}

	protected static void loadIntoCloud(String file) throws Exception {
//		String[] args = { Sp2b.class.getClassLoader().getResource(file).toString().replace("file:", "") , "1" , "1", "2100000"};
//		HBaseLoader.main(args);
	}

	protected static CloudEvaluator initCloudEvaluator() throws Exception {
		CloudEvaluator ev = new CloudEvaluator();
		return ev;
	}

	protected QueryResult executeQuery(BasicIndexQueryEvaluator evaluator,
			String query) throws Exception {
		if (evaluator instanceof CloudEvaluator) {
			evaluator.prepareInputData(new LinkedList<URILiteral>(),
					new LinkedList<URILiteral>()); // / workaround weil sonst
													// nullpointerexception
		}
		evaluator.compileQuery(query);
		evaluator.logicalOptimization();
		evaluator.physicalOptimization();

		return evaluator.getResult();
	}


	protected String readFile(String file) {
		InputStream is = getClass().getClassLoader().getResourceAsStream(file);
		String selectQuery = convertStreamToString(is);
		return selectQuery;
	}
	
	public static String convertStreamToString(java.io.InputStream is) {
		java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
		return s.hasNext() ? s.next() : "";
	}


}
