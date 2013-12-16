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

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.LinkedList;

import lupos.cloud.hbase.HBaseConnection;
import lupos.cloud.query.CloudEvaluator;
import lupos.datastructures.items.literal.LiteralFactory;
import lupos.datastructures.items.literal.URILiteral;

/**
 * Mit dieser Klasse und einen angegebenen Paramater ist es möglich effizient,
 * ohne den Umweg über die GUI, Tripel in HBase zu laden. Man hat die Wahl
 * zwischen zwei Modi wie die Daten in HBase geladen werden. Einmal per HBase
 * API und einmal per MapReduce Job. Die MapReduce Job Variante bietet sich
 * dann an wenn man eine größere Menge an Tripel laden will.
 */
public class HBaseLoader {

	/**
	 * Main Methode.
	 * 
	 * @param args
	 *            the arguments
	 * @throws Exception
	 *             the exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			System.out.println("Parameter: <n3 Pfad> <1/2 für normal oder BulkLoad> <HTriple Cache Size>");
			System.exit(0);
		}
		
		// init
		CloudEvaluator evaluator = new CloudEvaluator();
		if(args[1].equals("1")) {
			HBaseConnection.MAP_REDUCE_BULK_LOAD = false;
		} else {
			HBaseConnection.MAP_REDUCE_BULK_LOAD = true;
		}
		
		HBaseConnection.ROW_BUFFER_SIZE = Integer.parseInt(args[3]);
		HBaseConnection.deleteTableOnCreation = true;
		HBaseConnection.init();
		
		String file_path = args[0];
		FileReader fr = new FileReader(file_path);
		BufferedReader br = new BufferedReader(fr);

		StringBuilder prefix = new StringBuilder();
		String curLine = br.readLine();
		
		// Prefix merken
		while (curLine != null && curLine.startsWith("@")) {
			prefix.append(curLine);
			curLine = br.readLine();
		}
		
		long startTime = System.currentTimeMillis();
		long tripleAnzahl = 0;
		boolean run = true;
		
		// Tripel in 1000 Blöcke einlesen und den Evaluator übergeben
		while (run) {
			StringBuilder inputCache = new StringBuilder();

			for (int i = 0; i < 1000 && curLine != null; i++) {
				tripleAnzahl++;
				inputCache.append(curLine);
				curLine = br.readLine();
			}

			final URILiteral rdfURL = LiteralFactory
					.createStringURILiteral("<inlinedata:" + prefix.toString()
							+ "\n" + inputCache.toString() + ">");
			LinkedList<URILiteral> defaultGraphs = new LinkedList<URILiteral>();
			defaultGraphs.add(rdfURL);

			evaluator.prepareInputData(defaultGraphs,
					new LinkedList<URILiteral>());
			
			if (curLine == null) {
				run = false;
				break;
			}

		}
		br.close();

		HBaseConnection.flush();
		HBaseConnection.MAP_REDUCE_BULK_LOAD = false;
		HBaseConnection.deleteTableOnCreation = false;
		long stopTime = System.currentTimeMillis();
		System.out.println("Import ist beendet Triple: " + tripleAnzahl
				+ " Dauer: " + (stopTime - startTime) / 1000 + "s");
	}
}
