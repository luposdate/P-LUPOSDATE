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
package lupos.cloud.bloomfilter.mapreduce;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.NavigableMap;

import lupos.cloud.bloomfilter.BitvectorManager;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.xerial.snappy.Snappy;

/**
 * Innerhalb dieser Mapper-Klasse befindet sich die eigentliche Logik zum
 * erzeugen der Byte-Bitvektoren. Für jede in HBase wird die map()-Funktion
 * aufgerufen. Dort wird jeweils überprüft ob die Anzahl der Spalten-Elemente
 * größer als 25000 ist. Da jedoch nicht die gesamte Reihe auf einmal geladen
 * werden kann müssen die verschiedenen "batches" addiert werden und danach
 * erfolgt dann die Üebrprüfung ob die Reihe die Bedingung erfüllt. Ist die
 * Anzahl der Reihen kleiner als die maximale Anzahl Batch-Anzahl wird die Reihe
 * sofort verworfen.
 */
public class MyMapper extends TableMapper<ImmutableBytesWritable, Put> {

	/** Rowkey des letzten Durchlaufs. */
	byte[] lastRowkey = null;

	/** Bitvektor des ersten Elements. */
	BitSet bitvector1 = new BitSet(BitvectorManager.VECTORSIZE);

	/** Bitvektor des zweiten Elements. */
	BitSet bitvector2 = new BitSet(BitvectorManager.VECTORSIZE);

	/** Rowkey, für den der BV gespeichert werden oll */
	byte[] curBitvectorName = null;

	/** Bei setzetn dieser Variable werden die Bitvektore resetet. */
	boolean reset = true;

	/*
	 * (non-Javadoc)
	 *
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
	 * org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	public void map(final ImmutableBytesWritable row, final Result res, final Context context)
			throws IOException, InterruptedException {
		// Wenn nur sehr wenige Elemente in der Reihe vorhanden sind,
		// ueberspringe diese
		final int curColSize = res.getFamilyMap(
				BitvectorManager.bloomfilter1ColumnFamily).size();

		if (curColSize < BloomfilterGeneratorMR.BATCH - 1
				&& !Arrays.equals(this.lastRowkey, res.getRow())) {
			this.lastRowkey = res.getRow();
			context.getCounter("MyMapper", "SKIP_ROW").increment(1);
			return;
		}

		// Speichere Bitvektoren
		if (this.lastRowkey != null && !Arrays.equals(this.lastRowkey, res.getRow())) {
			if (this.bitvector1.cardinality() >= BloomfilterGeneratorMR.MIN_CARD) {
				final NavigableMap<byte[], byte[]> cfResults = res
						.getFamilyMap(BitvectorManager.bloomfilter1ColumnFamily);
				// store bitvectors
				this.storeBitvectorToHBase(this.curBitvectorName, this.bitvector1, this.bitvector2,
						context);
			}
			// reset
			this.reset = true;
		}

		final String curKey = Bytes.toString(res.getRow());
		if (this.reset) {
			this.curBitvectorName = res.getRow();
			this.bitvector1.clear();
			this.bitvector2.clear();
			this.reset = false;
		}

		if (curKey.contains(",")) {
			addResultToBitSet(false, this.bitvector1, this.bitvector2, res, context);
		} else {
			addResultToBitSet(true, this.bitvector1, this.bitvector2, res, context);
		}

		this.lastRowkey = res.getRow();
	}

	/**
	 * Diese Methode wird am Ende des Jobs aufgerufen. In dem Fall wird der
	 * Bitvektor der letzten Relevanten Reihe übertragen.
	 *
	 * @param context
	 *            the context
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 * @throws InterruptedException
	 *             the interrupted exception
	 */
	@Override
	protected void cleanup(final Context context) throws IOException,
			InterruptedException {
		if (this.curBitvectorName != null) {
			context.getCounter("MyMapper", "ADD_BYTE_BITVEKTOR").increment(1);
			// finally
			this.storeBitvectorToHBase(this.curBitvectorName, this.bitvector1, this.bitvector2,
					context);
		}
	}

	/**
	 * Speichert den Bitvektor (komprimiert mit Snappy) in HBase ab.
	 *
	 * @param curBitvectorName2
	 *            the cur bitvector name2
	 * @param bitvector1
	 *            the bitvector1
	 * @param bitvector2
	 *            the bitvector2
	 * @param context
	 *            the context
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 * @throws InterruptedException
	 *             the interrupted exception
	 */
	private void storeBitvectorToHBase(final byte[] curBitvectorName2,
			final BitSet bitvector1, final BitSet bitvector2, final Context context)
			throws IOException, InterruptedException {
		final Put row = new Put(curBitvectorName2);

		row.setWriteToWAL(false);

		final byte[] compressedBitvector1 = Snappy.compress(toByteArray(bitvector1));
		row.add(BitvectorManager.bloomfilter1ColumnFamily,
				Bytes.toBytes("bloomfilter"), compressedBitvector1);

		if (bitvector2.cardinality() > 0) {
			final byte[] compressedBitvector2 = Snappy
					.compress(toByteArray(bitvector2));
			row.add(BitvectorManager.bloomfilter2ColumnFamily,
					Bytes.toBytes("bloomfilter"), compressedBitvector2);
		}
		context.getCounter("MyMapper", "ADD_BYTE_BITVEKTOR").increment(1);
		final ImmutableBytesWritable key = new ImmutableBytesWritable(
				curBitvectorName2);
		context.write(key, row);
	}

	/**
	 * Setzt die Indizes der jeweiligen Zeile im aktuellen Bitvektor.
	 *
	 * @param twoBitvectors
	 *            the two bitvectors
	 * @param bitvector1
	 *            the bitvector1
	 * @param bitvector2
	 *            the bitvector2
	 * @param res
	 *            the res
	 * @param context
	 *            the context
	 * @throws UnsupportedEncodingException
	 *             the unsupported encoding exception
	 */
	private static void addResultToBitSet(final Boolean twoBitvectors,
			final BitSet bitvector1, final BitSet bitvector2, final Result res, final Context context)
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
				} else {
					context.getCounter("MyMapper", "BITVECTOR_EXIST_ALREADY")
							.increment(1);
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
					} else {
						context.getCounter("MyMapper",
								"BITVECTOR_EXIST_ALREADY").increment(1);
					}
				}
			}
		}
	}

	/**
	 * BitSet -> ByteArray
	 *
	 * @param bits
	 *            the bits
	 * @return the byte[]
	 */
	public static byte[] toByteArray(final BitSet bits) {
		return bits.toByteArray();
	}

	/**
	 * ByteArray -> BitSet.
	 *
	 * @param arr
	 *            the arr
	 * @return the integer
	 */
	private static Integer byteArrayToInteger(final byte[] arr) {
		return ByteBuffer.wrap(arr).getInt();
	}
}
