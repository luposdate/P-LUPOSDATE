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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;

import lupos.cloud.hbase.HBaseConnection;
import lupos.cloud.pig.PigQuery;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.bloom.HashFunction;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import org.apache.pig.impl.util.MultiMap;
import org.xerial.snappy.Snappy;

/**
 * Diese Klasse verwaltet alle Informationen bzgl. des Bitvektors. So wird z.B.
 * in dieser Klasse die Bitvektorgroesse gespeichert.
 */
public class BitvectorManager {

	/** Referenzen zu den jeweiligen HBase-Tabellen. */
	public static HashMap<String, HTable> hTables = new HashMap<String, HTable>();

	/** Bitvektorgroesse. */
	public static final int VECTORSIZE = 1000000000;

	/** Column-Family unter dem der Bitvektor 1 gespeichert werden soll. */
	public static final byte[] bloomfilter1ColumnFamily = "1".getBytes();

	/** Column-Family unter dem der Bitvektor 2 gespeichert werden soll. */
	public static final byte[] bloomfilter2ColumnFamily = "2".getBytes();

	/** Die für den Bitvektor verwendete Hash-Funktion. */
	private static HashFunction hash = new HashFunction(VECTORSIZE, 1,
			Hash.JENKINS_HASH);

	/** Speicherort der BItvektor auf dem verteilten Dateisystem. */
	public static final String WORKING_DIR = "/tmp/CloudBitvectors";

	/** Name des serialierten Bitvektor auf dem verteilten Dateisystem. */
	public static final String BLOOMFILTER_NAME = "cloudBloomfilter_";

	/**
	 * Hashfunktion.
	 *
	 * @param toHash
	 *            the to hash
	 * @return the int
	 */
	public static int hash(final byte[] toHash) {
		return hash.hash(new Key(toHash))[0];
	}

	/**
	 * Innerhalb dieser Methode wird der finale Bitvektor generiert. Dazu wird
	 * für jede Variable der zugehörige Bitvektor geladen und ungf. mit anderne
	 * Bitvektoren zum finalen Bitvektor verknüpft.
	 *
	 * @param bitvectors
	 *            the bitvectors
	 * @param pigQuery
	 *            the pig query
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static void generateBitvector(
			final HashMap<String, HashSet<CloudBitvector>> bitvectors,
			final PigQuery pigQuery) throws IOException {
		System.out.println("# bitvectors: " + bitvectors.size());

		init();
		for (final String var : bitvectors.keySet()) {

			final MultiMap<Integer, BitSet> bitSetList = new MultiMap<Integer, BitSet>();
			final MultiMap<Integer, Boolean> bitSetFromBytearrayList = new MultiMap<Integer, Boolean>();
			if (bitvectors.get(var).size() > 1) {
				for (final CloudBitvector bv : bitvectors.get(var)) {
					BitSet toAdd = null;
					Boolean fromBytearray = false;

					// lade Byte-Bitvektor. Wenn keiner existiert ist toAdd =
					// null
					toAdd = getDirectBitSetFromeHbaseTable(bv.getTablename(),
							bv.getRow(), bv.getColumnFamily());

					// generiere Bitvektor aus Indizes
					if (toAdd == null) {
						toAdd = getBitSetFromeHbaseTable(bv.getTablename(),
								bv.getRow(), bv.getColumnFamily());
					} else {
						fromBytearray = true;
					}

					bitSetList.put(bv.getSetId(), toAdd);
					bitSetFromBytearrayList.put(bv.getSetId(), fromBytearray);
				}
			}

			// Wenn nur ein bitvector vorahnden ist ignoriere diesen
			if (bitSetList.size() == 0) {
				System.out.println("\n---> " + var
						+ " vector ignored, because appears only once <---");
				for (final CloudBitvector bv : bitvectors.get(var)) {
					pigQuery.replaceBloomfilterName(
							DigestUtils.sha512Hex(var + bv.getPatternId())
									.toString(), WORKING_DIR + "/"
									+ BLOOMFILTER_NAME + var.replace("?", "")
									+ "_IGNORE");
				}
			} else {
				// AND verknüpfen
				final ArrayList<BitSet> groupBitSetList = new ArrayList<BitSet>();
				Integer startId = null;
				boolean first = true;
				for (final Integer setId : bitSetList.keySet()) {
					if (first) {
						startId = setId;
						first = false;
					}
					groupBitSetList.add(mergeBitSet(var, bitSetList.get(setId),
							bitSetFromBytearrayList.get(setId)));
				}

				Integer groupCounter = startId;
				for (final BitSet bitVector : groupBitSetList) {
					final Path local = new Path("cloudBloomfilter_"
							+ var.replace("?", "") + "_" + groupCounter);
					final Path remote = new Path(WORKING_DIR + "/" + BLOOMFILTER_NAME
							+ var.replace("?", "") + "_" + groupCounter);
					// sind mehr als 95% der Bits gesetzt -> ignoiere den
					// Bitvektor
					if ((bitVector.cardinality()) >= (BitvectorManager.VECTORSIZE * 0.95)) {
						System.out
								.println("\n---> "
										+ var
										+ " vector ignored, because to many true bits (>95%) <---");
						for (final CloudBitvector bv : bitvectors.get(var)) {
							pigQuery.replaceBloomfilterName(DigestUtils
									.sha512Hex(var + bv.getPatternId())
									.toString(), WORKING_DIR + "/"
									+ BLOOMFILTER_NAME + var.replace("?", "")
									+ "_IGNORE");
						}
					} else {
						writeByteToDisk(toByteArray(bitVector), local);
						HBaseConnection.getHdfs_fileSystem().copyFromLocalFile(
								true, true, local, remote);
						new File(local.getName()).delete();

						// Ersetze Hash-Strings im PigLatin-Programm
						for (final CloudBitvector bv : bitvectors.get(var)) {
							if (bv.getSetId() == groupCounter) {
								pigQuery.replaceBloomfilterName(DigestUtils
										.sha512Hex(var + bv.getPatternId())
										.toString(),
										WORKING_DIR + "/" + BLOOMFILTER_NAME
												+ var.replace("?", "") + "_"
												+ groupCounter);
							}
						}
					}
					groupCounter++;

				}
			}
		}

		// clean up

		for (final HTable tab : hTables.values()) {
			tab.close();
		}
		hTables = new HashMap<String, HTable>();

	}

	/**
	 * Init.
	 *
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	private static void init() throws IOException {
		HBaseConnection.init();
		HBaseConnection.getHdfs_fileSystem()
				.delete(new Path(WORKING_DIR), true);
		HBaseConnection.getHdfs_fileSystem().mkdirs(new Path(WORKING_DIR));

	}

	/**
	 * Diese Methode lädt den Bitvektor aus HBase und erzeugt das BitSet anhand
	 * der einzelnen Indizes zurück in ein Java-Bitset.
	 *
	 * @param tablename
	 *            the tablename
	 * @param row
	 *            the row
	 * @param cf
	 *            the cf
	 * @return the bit set frome hbase table
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	private static BitSet getBitSetFromeHbaseTable(final String tablename,
			final String row, final byte[] cf) throws IOException {

		final BitSet bitvector = new BitSet(VECTORSIZE);

		// Spezialfall bei ?s ?p ?o, es gibt kein Bitvector für ?s, da rowkey,
		// daher wird ein volller bitvector zurück gegeben
		if (cf == null) {
			bitvector.set(0, VECTORSIZE);
			return bitvector;
		}

		HTable hTable = hTables.get(tablename);
		if (hTable == null) {
			hTable = new HTable(HBaseConnection.getConfiguration(), tablename);
			hTables.put(tablename, hTable);
		}

		final Scan s = new Scan();
		s.setStartRow(Bytes.toBytes(row));
		s.setStopRow(Bytes.toBytes(row + "z"));
		s.setBatch(250000);
		s.setCacheBlocks(false);
		s.addFamily(cf);

		final ResultScanner scanner = hTable.getScanner(s);
		for (Result res = scanner.next(); res != null; res = scanner.next()) {
			addResultToBitSet(bitvector, res, cf);
		}

		// cleanup
		// hTable.close();

		return bitvector;
	}

	/**
	 * In dieser Methode wird der serialisierte Byte-Bitvektor aus HBase geladen
	 * und in das Java BitSet-Objekt überführt.
	 *
	 * @param tablename
	 *            the tablename
	 * @param row
	 *            the row
	 * @param cf
	 *            the cf
	 * @return the direct bit set frome hbase table
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	private static BitSet getDirectBitSetFromeHbaseTable(final String tablename,
			final String row, final byte[] cf) throws IOException {
		BitSet result = null;

		// Spezialfall, wird in anderer Methode verarbeitet
		if (cf == null) {
			return null;
		}

		HTable hTable = hTables.get(tablename);
		if (hTable == null) {
			hTable = new HTable(HBaseConnection.getConfiguration(), tablename);
			hTables.put(tablename, hTable);
		}

		final Get g = new Get(row.getBytes());
		g.addColumn(cf, "bloomfilter".getBytes());
		final Result r = hTable.get(g);

		if (!r.isEmpty()) {
			result = fromByteArray(r.getValue(cf, "bloomfilter".getBytes()));
			hTable.close();
		}

		return result;
	}

	/**
	 * Setzt die Indizes im Bitvektor.
	 *
	 * @param bitvector
	 *            the bitvector
	 * @param resultMap
	 *            the result map
	 * @param cf
	 *            the cf
	 * @return the bit set
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static BitSet addResultToBitSet(final BitSet bitvector, final Result resultMap,
			final byte[] cf) throws IOException {
		final NavigableMap<byte[], byte[]> cfResults = resultMap.getFamilyMap(cf);
		if (cfResults != null) {
			for (final byte[] entry : cfResults.keySet()) {
				final Integer pos = byteArrayToInteger(entry);
				bitvector.set(pos);
			}
		}
		return bitvector;
	}

	/**
	 * Byte -> Integer.
	 *
	 * @param arr
	 *            the arr
	 * @return the integer
	 */
	private static Integer byteArrayToInteger(final byte[] arr) {
		return ByteBuffer.wrap(arr).getInt();
	}

	/**
	 * Schreibt ein beliebiges Byte-Array auf das HDFS-Dateisystem.
	 *
	 * @param toWrite
	 *            the to write
	 * @param path
	 *            the path
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static void writeByteToDisk(final byte[] toWrite, final Path path)
			throws IOException {
		final FileOutputStream fos = new FileOutputStream(path.getName());
		fos.write(toWrite);
		fos.flush();
		fos.close();
	}

	/**
	 * UND-Verkünpfung der Bitvektoren.
	 *
	 * @param var
	 *            the var
	 * @param bitSetList
	 *            the bit set list
	 * @param bytearrayBooleanList
	 *            the bytearray boolean list
	 * @return the bit set
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static BitSet mergeBitSet(final String var, final List<BitSet> bitSetList,
			final List<Boolean> bytearrayBooleanList) throws IOException {
		if (bitSetList.size() == 1) {
			return bitSetList.get(0);
		}
		System.out.print("\n---> " + var + " is merged (and) from ");
		int j = 0;
		for (final BitSet bs : bitSetList) {
			if (j > 0) {
				System.out.print(", ");
			}
			final int card = bs.cardinality();

			if (bytearrayBooleanList.get(j)) {
				System.out.print(card + "(b)");
			} else {
				System.out.print(card);
			}
			j++;
		}

		for (int i = 1; i < bitSetList.size(); i++) {
			bitSetList.get(0).and(bitSetList.get(i));
		}

		System.out.println(" to " + bitSetList.get(0).cardinality() + " <---");
		return bitSetList.get(0);
	}

	/**
	 * BitSet -> Byte-Array. Zusätlich findet die Snappy-Komprimierung hier
	 * statt.
	 *
	 * @param bits
	 *            the bits
	 * @return the byte[]
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static byte[] toByteArray(final BitSet bits) throws IOException {
		final byte[] compressedBytes = Snappy.compress(bits.toByteArray());
		return compressedBytes;
	}

	/**
	 * Byte-Array -> BitSet. Zusätzlich findet die Snappy-Dekomprimierung hier
	 * statt.
	 *
	 * @param compressedBytes
	 *            the compressed bytes
	 * @return the bit set
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static BitSet fromByteArray(final byte[] compressedBytes)
			throws IOException {
		final byte[] bytes = Snappy.uncompress(compressedBytes);
		return BitSet.valueOf(bytes);
	}
}