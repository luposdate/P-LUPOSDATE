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
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.util.Iterator;

import lupos.datastructures.items.Triple;
import lupos.rdf.parser.NquadsParser;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

/**
 * Dieser Converter übersetzt Quad Tripel in N3 Tripel.
 */
public class QuadToN3Converter {

	/**
	 * Main Methode.
	 *
	 * @param args
	 *            the arguments
	 * @throws Exception
	 *             the exception
	 */
	public static void main(final String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Parameter: <quad_input_file> <quad_output_file>");
			System.exit(0);
		}

		final InputStream is = QuadToN3Converter.class.getClassLoader()
				.getResourceAsStream(args[0]);
		final PrintWriter writer = new PrintWriter(args[1], "UTF-8");

		final NxParser nxp = new NxParser();


		final long startTime = System.currentTimeMillis();
		int number = 0;
		final Iterator<Node[]> it = nxp.parse(is);
		while (it.hasNext()) {
			final Node[] ns = it.next();
			number++;
			if (number % 1000000 == 0){
				System.out.println("#triples:" + number);
			}
			try {
				writer.println(new Triple(NquadsParser.transformToLiteral(ns[0]),
						NquadsParser.transformToLiteral(ns[1]),
						NquadsParser.transformToLiteral(ns[2])).toN3String());
			} catch (final URISyntaxException e) {
				System.err.println(e);
				e.printStackTrace();
			}
		}

		writer.close();
		final long stopTime = System.currentTimeMillis();
		System.out.println("Generierte Tripel " + number
				+ " Dauer: " + (stopTime - startTime) / 1000 + "s");
	}
}
