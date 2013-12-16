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
package lupos.cloud.bloomfilter;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.NavigableMap;

import lupos.cloud.hbase.HBaseConnection;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Mit Hilfe dieser Klasse wird die Byte-Bitvektorgeneierung gestartet. Dabei
 * wird die Verbindung direkt über die HBase API hergestellt und jeder Rowkey
 * durchgegangen.
 *
 * @deprecated Kommunikation per HBase-Api ist zu langsam. Besser BloomfilterGeneratorMR verwenden.
 */
@Deprecated
public class BloomfilterGenerator {

	/** The min card. */
	private static Integer MIN_CARD = 100;

	/**
	 * The main method.
	 *
	 * @param args
	 *            the arguments
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static void main(final String[] args) throws IOException {
		if (args.length != 2) {
			System.out.println("java -jar programm <batchSize> <CachingSize>");
			System.exit(0);
		}

		final int batchSize = Integer.parseInt(args[0]);
		final int cachingSize = Integer.parseInt(args[1]);

		HBaseConnection.init();
		int bitvectorCount = 0;
		final long startTime = System.currentTimeMillis();
		long checkedNumber = 0;

		// for (String tablename : HBaseDistributionStrategy.getTableInstance()
		// .getTableNames()) {
		final String tablename = "PO_S";
		System.out.println("Aktuelle Tabelle: " + tablename);
		final HTable hTable = new HTable(HBaseConnection.getConfiguration(),
				tablename);

		final Scan s = new Scan();
		s.setBatch(batchSize);
		s.setCaching(cachingSize);
		s.setCacheBlocks(true);

		s.addFamily(BitvectorManager.bloomfilter1ColumnFamily);
		s.addFamily(BitvectorManager.bloomfilter2ColumnFamily);

		final ResultScanner scanner = hTable.getScanner(s);

		byte[] lastRowkey = null;
		final BitSet bitvector1 = new BitSet(BitvectorManager.VECTORSIZE);
		final BitSet bitvector2 = new BitSet(BitvectorManager.VECTORSIZE);
		boolean reset = true;
		byte[] curBitvectorName = null;
		for (Result res = scanner.next(); res != null; res = scanner.next()) {
			// Ausgabe der momentanen Position
			if (checkedNumber % 1000000 == 0) {
				System.out.println(checkedNumber + " Rows checked");
			}
			checkedNumber++;

			// Wenn nur sehr wenige Elemente in der Reihe vorhanden sind,
			// ueberspringe diese
			final int curColSize = res.getFamilyMap(
					BitvectorManager.bloomfilter1ColumnFamily).size();

			if (curColSize < batchSize
					&& !Arrays.equals(lastRowkey, res.getRow())) {
				lastRowkey = res.getRow();
				continue;
			}

			// Speichere Bitvektoren
			if (lastRowkey != null && !Arrays.equals(lastRowkey, res.getRow())) {
				if (bitvector1.cardinality() >= MIN_CARD) {
					// store bitvectors
					storeBitvectorToHBase(tablename, curBitvectorName,
							bitvector1, bitvector2, hTable);
					bitvectorCount++;
				}
				// reset
				reset = true;
			}

			final String curKey = Bytes.toString(res.getRow());
			if (reset) {
				curBitvectorName = res.getRow();
				bitvector1.clear();
				bitvector2.clear();
				reset = false;
			}

			if (curKey.contains(",")) {
				addResultToBitSet(false, bitvector1, bitvector2, res);
			} else {
				addResultToBitSet(true, bitvector1, bitvector2, res);
			}

			lastRowkey = res.getRow();
		}

		// letzten Bitvektor speichern
		if (lastRowkey != null) {
			if (bitvector1.cardinality() >= MIN_CARD) {
				storeBitvectorToHBase(tablename, curBitvectorName, bitvector1,
						bitvector2, hTable);
			}
		}

		// cleanup
		scanner.close();
		hTable.close();
		// } // close
		final long stopTime = System.currentTimeMillis();
		System.out
				.println("Bitvektor Generierung beendet. Anzahl der erzeugten Bitvektoren: "
						+ bitvectorCount
						+ " Dauer: "
						+ (stopTime - startTime)
						/ 1000 + "s");
	}


	/**
	 * Adds the result to bit set.
	 *
	 * @param twoBitvectors
	 *            the two bitvectors
	 * @param bitvector1
	 *            the bitvector1
	 * @param bitvector2
	 *            the bitvector2
	 * @param res
	 *            the res
	 * @throws UnsupportedEncodingException
	 *             the unsupported encoding exception
	 */
	private static void addResultToBitSet(final Boolean twoBitvectors,
			final BitSet bitvector1, final BitSet bitvector2, final Result res)
			throws UnsupportedEncodingException {
		final byte[] bloomfilterColumn = "bloomfilter".getBytes();

		// Bitvektor 1
		NavigableMap<byte[], byte[]> cfResults = res
				.getFamilyMap(BitvectorManager.bloomfilter1ColumnFamily);
		if (cfResults != null) {
			for (final byte[] entry : cfResults.keySet()) {
				// Bloomfilter
				if (!Arrays.equals(entry, bloomfilterColumn)) {
					final Integer position = byteArrayToInteger(entry);
					bitvector1.set(position);
				}
			}
		}

		// Bitvektor 2
		if (twoBitvectors) {
			cfResults = res
					.getFamilyMap(BitvectorManager.bloomfilter2ColumnFamily);
			if (cfResults != null) {
				for (final byte[] entry : cfResults.keySet()) {
					// Bloomfilter
					if (!Arrays.equals(entry, bloomfilterColumn)) {
						final Integer position = byteArrayToInteger(entry);
						bitvector2.set(position);
					}
				}
			}
		}
	}

	/**
	 * Byte array to integer.
	 *
	 * @param arr
	 *            the arr
	 * @return the integer
	 */
	private static Integer byteArrayToInteger(final byte[] arr) {
		return ByteBuffer.wrap(arr).getInt();
	}

	/**
	 * Store bitvector to h base.
	 *
	 * @param tablename
	 *            the tablename
	 * @param rowkey
	 *            the rowkey
	 * @param bitvector1
	 *            the bitvector1
	 * @param bitvector2
	 *            the bitvector2
	 * @param table
	 *            the table
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	private static void storeBitvectorToHBase(final String tablename, final byte[] rowkey,
			final BitSet bitvector1, final BitSet bitvector2, final HTable table)
			throws IOException {
		// HTable table = new HTable(HBaseConnection.getConfiguration(),
		// tablename);
		final Put row = new Put(rowkey);
		row.add(BitvectorManager.bloomfilter1ColumnFamily,
				Bytes.toBytes("bloomfilter"), toByteArray(bitvector1));
		if (bitvector2 != null) {
			row.add(BitvectorManager.bloomfilter2ColumnFamily,
					Bytes.toBytes("bloomfilter"), toByteArray(bitvector2));
		}
		table.put(row);
		// table.close();
		System.out.println("Tabelle : " + tablename + " RowKey: "
				+ Bytes.toString(rowkey) + " Bitvector-Size: "
				+ bitvector1.cardinality());
	}

	/**
	 * To byte array.
	 *
	 * @param bits
	 *            the bits
	 * @return the byte[]
	 */
	public static byte[] toByteArray(final BitSet bits) {
		return bits.toByteArray();
	}

}
