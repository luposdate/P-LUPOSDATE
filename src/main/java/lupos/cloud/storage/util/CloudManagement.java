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
package lupos.cloud.storage.util;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

import lupos.cloud.bloomfilter.BitvectorManager;
import lupos.cloud.hbase.HBaseConnection;
import lupos.cloud.hbase.HBaseDistributionStrategy;
import lupos.cloud.hbase.HBaseTriple;
import lupos.cloud.pig.PigQuery;
import lupos.datastructures.bindings.Bindings;
import lupos.datastructures.bindings.BindingsFactory;
import lupos.datastructures.items.Variable;
import lupos.datastructures.items.literal.LiteralFactory;
import lupos.datastructures.queryresult.QueryResult;
import lupos.misc.util.ImmutableIterator;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;

/**
 * Diese Klasse ist für die Kommunikation mit der Cloud zuständig (sowohl HBase
 * als auch MapReduce/Pig).
 */
public class CloudManagement {

	/** Anzahl der geladenen Tripel. */
	public static long countTriple = 0;

	/** PigServer Referenzs. */
	static PigServer pigServer = null;

	/** Query Result. */
	Iterator<Tuple> pigQueryResult = null;

	/** Variablenliste. */
	ArrayList<String> curVariableList = null;

	/** falls true wird das generierte PigLatin-Programm ausgegeben */
	boolean PRINT_PIGLATIN_PROGRAMM = false;

	/** Testing Modus. Wenn aktiv, wird das Programm nicht versendet. */
	boolean TESTING_MODE = false;

	/** # Reduce Knoten. */
	public static int PARALLEL_REDUCE_OPERATIONS = 5;

	/** Dauer der Bitvektorgeneierung. */
	public double bitvectorTime = 0;

	/** Aktiviert den Bloomfilter. */
	public static boolean bloomfilter_active = true;

	/** Zeit Formatierung. */
	static SimpleDateFormat formatter = new SimpleDateFormat(
			"yyyy.MM.dd HH:mm:ss");

	/**
	 * Instantiates a new cloud management.
	 */
	public CloudManagement() {

		if (this.TESTING_MODE) {
			return;
		}
		try {
			HBaseConnection.init();
			pigServer = new PigServer(ExecType.MAPREDUCE);
			for (final String tablename : HBaseDistributionStrategy
					.getTableInstance().getTableNames()) {
				HBaseConnection.createTable(tablename,
						HBaseDistributionStrategy.getTableInstance()
								.getColumnFamilyName());
			}
		} catch (final IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Submit h base triple to database.
	 *
	 * @param triple
	 *            the triple
	 */
	public void submitHBaseTripleToDatabase(final Collection<HBaseTriple> triple) {
		for (final HBaseTriple item : triple) {
			if (countTriple % 1000000 == 0) {
				if (countTriple != 0) {
					System.out.println(formatter.format(new Date()).toString()
							+ ": " + countTriple + " HBaseTripel importiert!");
				}
			}
			try {
				HBaseConnection.addRow(item.getTablename(), item.getRow_key(),
						item.getColumnFamily(), item.getColumn(),
						item.getValue());
				countTriple++;
			} catch (final IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Delete h base triple from database.
	 *
	 * @param triple
	 *            the triple
	 */
	public void deleteHBaseTripleFromDatabase(
			final Collection<HBaseTriple> triple) {
		try {
			for (final HBaseTriple item : triple) {
				HBaseConnection.deleteRow(item.getTablename(),
						item.getColumnFamily(), item.getRow_key(),
						item.getColumn());
			}
		} catch (final IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Submit pig query.
	 *
	 * @param query
	 *            the query
	 * @return the query result
	 */
	public QueryResult submitPigQuery(final PigQuery query, final BindingsFactory bindingsFactory) {

		QueryResult result = null;
		final long start = System.currentTimeMillis();
		try {
			if (bloomfilter_active) {
				final long start2 = System.currentTimeMillis();
				BitvectorManager
						.generateBitvector(query.getBitvectors(), query);
				final long stop2 = System.currentTimeMillis();
				System.out.println("Bitvector generated in "
						+ new DecimalFormat("#.##")
								.format(((double) stop2 - (double) start2)
										/ 1000) + "s!");

				this.bitvectorTime = (stop2 - start2) / 1000.0;
			}
			if (this.PRINT_PIGLATIN_PROGRAMM) {
				System.out.println("Generated PigLatin Program:");
				System.out.println(query.getPigLatin());
				System.out.println();
			}
			//
			if (this.TESTING_MODE)
			 {
				return null; // testing purpose
			}
			System.out.println("PigLatin Programm wird ausgefuehrt...");
			pigServer.registerQuery(query.getPigLatin());
			this.curVariableList = query.getVariableList();
			this.pigQueryResult = pigServer.openIterator("X");
			result = new QueryResult();
			result = QueryResult
					.createInstance(new ImmutableIterator<Bindings>() {
						public boolean hasNext() {
							return CloudManagement.this.pigQueryResult.hasNext();
						}

						public Bindings next() {
							if (this.hasNext()) {
								try {
									final Bindings result = bindingsFactory.createInstance();
									final Tuple tuple = CloudManagement.this.pigQueryResult.next();
									int i = 0;
									for (final String var : CloudManagement.this.curVariableList) {
										final Object curTupleObject = tuple.get(i);
										// unbounded Variables
										if (curTupleObject == null) {
											// do nothing
											// result.add(new Variable(var),
											// null);

										} else {

											final String curTupel = curTupleObject
													.toString();
											if (curTupel.toString().startsWith(
													"<")) {
												result.add(
														new Variable(var),
														LiteralFactory
																.createURILiteral(tuple
																		.get(i)
																		.toString()
																		.substring(
																				0,
																				tuple.get(
																						i)
																						.toString()
																						.lastIndexOf(
																								">") + 1)));
											} else if (curTupel
													.startsWith("\"")) {
												final String content = curTupel.substring(
														curTupel.indexOf("\""),
														curTupel.lastIndexOf("\"") + 1);
												final int startIndex = curTupel
														.indexOf("<");
												final int stopIndex = curTupel
														.indexOf(">") + 1;
												if (startIndex != -1
														&& stopIndex != -1) {
													final String type = curTupel
															.substring(
																	startIndex,
																	stopIndex);
													result.add(
															new Variable(var),
															LiteralFactory
																	.createTypedLiteral(
																			content,
																			type));
												} else {
													result.add(
															new Variable(var),
															LiteralFactory
																	.createLiteral(content));
												}
											} else if (curTupel
													.startsWith("_:")) {
												result.add(
														new Variable(var),
														LiteralFactory
																.createAnonymousLiteral(curTupel));
											} else if (curTupel
													.startsWith("_:")) {
												result.add(
														new Variable(var),
														LiteralFactory
																.createAnonymousLiteral(curTupel));
											} else {
												result.add(
														new Variable(var),
														LiteralFactory
																.createLiteral(tuple
																		.get(i)
																		.toString()));
											}
										}
										i++;
									}
									return result;
								} catch (final Exception e) {
									e.printStackTrace();
									return null;
								}
							} else {
								return null;
							}
						}
					});
		} catch (final IOException e) {
			e.printStackTrace();
		}
		final long stop = System.currentTimeMillis();
		System.out.println("PigLatin Programm erfolgreich in "
				+ ((stop - start) / 1000) + "s ausgeführt!");
		return result;
	}

	/**
	 * Shutdown.
	 */
	public void shutdown() {
		pigServer.shutdown();
	}

	/**
	 * Gets the bitvector time.
	 *
	 * @return the bitvector time
	 */
	public double getBitvectorTime() {
		return this.bitvectorTime;
	}
}
