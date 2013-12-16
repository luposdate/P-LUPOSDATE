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
package lupos.cloud.pig.udfs;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Properties;

import lupos.cloud.bloomfilter.BitvectorManager;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadPushDown;
import org.apache.pig.LoadStoreCaster;
import org.apache.pig.OrderedLoadFunc;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.hbase.HBaseBinaryConverter;
import org.apache.pig.backend.hadoop.hbase.HBaseTableInputFormat.HBaseTableIFBuilder;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.joda.time.DateTime;

import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;

/**
 * Diese UDF Funktion ist eine angepasste Variante der originalen HBaseStorage()
 * UDF Funktion. Diese wurde zum größten Teil übernommen und an einigen Stellen
 * angepasst. Eine wichtige erweiterung ist die Möglichkeit nur einen bestimmten
 * rowKey zu laden, anstatt den gesamten Datenbestand einer Tabelle.
 *
 * Anmerkung: Wurde zu Testzwecken verwendet. Die Funktion lädt die Daten direkt
 * in eine Bag Datenstruktur. Jedoch ist die Implementierung sehr unschön
 * (setBatch = 1)
 */
@Deprecated
public class HBaseLoadBagUDF extends LoadFunc implements StoreFuncInterface,
		LoadPushDown, OrderedLoadFunc {

	private static final Log LOG = LogFactory.getLog(HBaseLoadBagUDF.class);

	private final static String STRING_CASTER = "UTF8StorageConverter";
	private final static String BYTE_CASTER = "HBaseBinaryConverter";
	private final static String CASTER_PROPERTY = "pig.hbase.caster";
	private final static String ASTERISK = "*";
	private final static String COLON = ":";
	private final static String HBASE_SECURITY_CONF_KEY = "hbase.security.authentication";
	private final static String HBASE_CONFIG_SET = "hbase.config.set";
	private final static String HBASE_TOKEN_SET = "hbase.token.set";

	private List<ColumnInfo> columnInfo_ = Lists.newArrayList();
	private HTable m_table;

	// Use JobConf to store hbase delegation token
	private JobConf m_conf;
	private RecordReader reader;
	private RecordWriter writer;
	private TableOutputFormat outputFormat = null;
	private Scan scan;
	private String contextSignature = null;

	private final CommandLine configuredOptions_;
	private final static Options validOptions_ = new Options();
	private final static CommandLineParser parser_ = new GnuParser();

	private boolean loadRowKey_;
	private String delimiter_;
	private boolean ignoreWhitespace_;
	private final long limit_;
	private final int caching_;
	private final boolean noWAL_;
	private final long minTimestamp_;
	private final long maxTimestamp_;
	private final long timestamp_;

	protected transient byte[] gt_;
	protected transient byte[] gte_;
	protected transient byte[] lt_;
	protected transient byte[] lte_;

	private LoadCaster caster_;

	private ResourceSchema schema_;
	private RequiredFieldList requiredFieldList;

	private FileSystem fs = null;
	private Path bitvectorPath1 = null;
	private Path bitvectorPath2 = null;

	private static void populateValidOptions() {
		validOptions_.addOption("loadKey", false, "Load Key");
		validOptions_.addOption("gt", true,
				"Records must be greater than this value "
						+ "(binary, double-slash-escaped)");
		validOptions_
				.addOption("lt", true,
						"Records must be less than this value (binary, double-slash-escaped)");
		validOptions_.addOption("gte", true,
				"Records must be greater than or equal to this value");
		validOptions_.addOption("lte", true,
				"Records must be less than or equal to this value");
		validOptions_.addOption("caching", true,
				"Number of rows scanners should cache");
		validOptions_.addOption("limit", true, "Per-region limit");
		validOptions_.addOption("delim", true, "Column delimiter");
		validOptions_.addOption("ignoreWhitespace", true,
				"Ignore spaces when parsing columns");
		validOptions_
				.addOption(
						"caster",
						true,
						"Caster to use for converting values. A class name, "
								+ "HBaseBinaryConverter, or Utf8StorageConverter. For storage, casters must implement LoadStoreCaster.");
		validOptions_
				.addOption(
						"noWAL",
						false,
						"Sets the write ahead to false for faster loading. To be used with extreme caution since this could result in data loss (see http://hbase.apache.org/book.html#perf.hbase.client.putwal).");
		validOptions_.addOption("minTimestamp", true,
				"Record must have timestamp greater or equal to this value");
		validOptions_.addOption("maxTimestamp", true,
				"Record must have timestamp less then this value");
		validOptions_.addOption("timestamp", true,
				"Record must have timestamp equal to this value");

	}

	/**
	 * Constructor. Construct a HBase Table LoadFunc and StoreFunc to load or
	 * store the cells of the provided columns.
	 *
	 * @param columnList
	 *            columnlist that is a presented string delimited by space
	 *            and/or commas. To retreive all columns in a column family
	 *            <code>Foo</code>, specify a column as either <code>Foo:</code>
	 *            or <code>Foo:*</code>. To fetch only columns in the CF that
	 *            start with <I>bar</I>, specify <code>Foo:bar*</code>. The
	 *            resulting tuple will always be the size of the number of
	 *            tokens in <code>columnList</code>. Items in the tuple will be
	 *            scalar values when a full column descriptor is specified, or a
	 *            map of column descriptors to values when a column family is
	 *            specified.
	 *
	 * @throws ParseException
	 *             when unable to parse arguments
	 * @throws IOException
	 */

	public HBaseLoadBagUDF(final String columnList, final String optString, final String rowKey,
			final String bitvectorPath) throws ParseException, IOException {
		this(columnList, optString, rowKey, new Path(bitvectorPath), null);

	}

	public HBaseLoadBagUDF(final String columnList, final String optString, final String rowKey,
			final String bitvectorPath1, final String bitvectorPath2)
			throws ParseException, IOException {
		this(columnList, optString, rowKey, new Path(bitvectorPath1), new Path(
				bitvectorPath2));

	}

	private byte[] readBloomfilterToByte(final Path path) {
		byte[] bitvector = null;
		try {
			if (this.fs == null) {
				this.fs = FileSystem.get(new Configuration());
			}
			final FSDataInputStream input = this.fs.open(path);
			bitvector = ByteStreams.toByteArray(input);
			// bitvector = longToBitSet(input.readLong());
			input.close();
		} catch (final IOException e) {
			e.printStackTrace();
		}
		return bitvector;
	}

	public static BitSet fromByteArray(final byte[] bytes) {
		return BitSet.valueOf(bytes);
	}

	/**
	 * Constructor. Construct a HBase Table LoadFunc and StoreFunc to load or
	 * store.
	 *
	 * @param columnList
	 * @param optString
	 *            Loader options. Known options:
	 *            <ul>
	 *            <li>-loadKey=(true|false) Load the row key as the first column
	 *            <li>-gt=minKeyVal
	 *            <li>-lt=maxKeyVal
	 *            <li>-gte=minKeyVal
	 *            <li>-lte=maxKeyVal
	 *            <li>-limit=numRowsPerRegion max number of rows to retrieve per
	 *            region
	 *            <li>-delim=char delimiter to use when parsing column names
	 *            (default is space or comma)
	 *            <li>-ignoreWhitespace=(true|false) ignore spaces when parsing
	 *            column names (default true)
	 *            <li>-caching=numRows number of rows to cache (faster scans,
	 *            more memory).
	 *            <li>-noWAL=(true|false) Sets the write ahead to false for
	 *            faster loading.
	 *            <li>-minTimestamp= Scan's timestamp for min timeRange
	 *            <li>-maxTimestamp= Scan's timestamp for max timeRange
	 *            <li>-timestamp= Scan's specified timestamp
	 *            <li>-caster=(HBaseBinaryConverter|Utf8StorageConverter)
	 *            Utf8StorageConverter is the default To be used with extreme
	 *            caution, since this could result in data loss (see
	 *            http://hbase.apache.org/book.html#perf.hbase.client.putwal).
	 *            </ul>
	 * @throws ParseException
	 * @throws IOException
	 */
	public HBaseLoadBagUDF(final String columnList, final String optString, final String rowKey,
			final Path bitvectorPath1, final Path bitvectorPath2) throws ParseException,
			IOException {
		populateValidOptions();

		this.bitvectorPath1 = bitvectorPath1;
		this.bitvectorPath2 = bitvectorPath2;

		final String[] optsArr = optString.split(" ");
		try {
			this.configuredOptions_ = parser_.parse(validOptions_, optsArr);
		} catch (final ParseException e) {
			final HelpFormatter formatter = new HelpFormatter();
			formatter
					.printHelp(
							"[-loadKey] [-gt] [-gte] [-lt] [-lte] [-columnPrefix] [-caching] [-caster] [-noWAL] [-limit] [-delim] [-ignoreWhitespace] [-minTimestamp] [-maxTimestamp] [-timestamp]",
							validOptions_);
			throw e;
		}

		this.loadRowKey_ = this.configuredOptions_.hasOption("loadKey");

		this.delimiter_ = ",";
		if (this.configuredOptions_.getOptionValue("delim") != null) {
			this.delimiter_ = this.configuredOptions_.getOptionValue("delim");
		}

		this.ignoreWhitespace_ = true;
		if (this.configuredOptions_.hasOption("ignoreWhitespace")) {
			final String value = this.configuredOptions_
					.getOptionValue("ignoreWhitespace");
			if (!"true".equalsIgnoreCase(value)) {
				this.ignoreWhitespace_ = false;
			}
		}

		this.columnInfo_ = this.parseColumnList(columnList, this.delimiter_, this.ignoreWhitespace_);

		final String defaultCaster = UDFContext.getUDFContext()
				.getClientSystemProps()
				.getProperty(CASTER_PROPERTY, STRING_CASTER);
		final String casterOption = this.configuredOptions_.getOptionValue("caster",
				defaultCaster);
		if (STRING_CASTER.equalsIgnoreCase(casterOption)) {
			this.caster_ = new Utf8StorageConverter();
		} else if (BYTE_CASTER.equalsIgnoreCase(casterOption)) {
			this.caster_ = new HBaseBinaryConverter();
		} else {
			try {
				this.caster_ = (LoadCaster) PigContext
						.instantiateFuncFromSpec(casterOption);
			} catch (final ClassCastException e) {
				LOG.error("Configured caster does not implement LoadCaster interface.");
				throw new IOException(e);
			} catch (final RuntimeException e) {
				LOG.error("Configured caster class not found.", e);
				throw new IOException(e);
			}
		}
		LOG.debug("Using caster " + this.caster_.getClass());

		this.caching_ = Integer.valueOf(this.configuredOptions_.getOptionValue("caching",
				"100"));
		this.limit_ = Long.valueOf(this.configuredOptions_.getOptionValue("limit", "-1"));
		this.noWAL_ = this.configuredOptions_.hasOption("noWAL");

		if (this.configuredOptions_.hasOption("minTimestamp")) {
			this.minTimestamp_ = Long.parseLong(this.configuredOptions_
					.getOptionValue("minTimestamp"));
		} else {
			this.minTimestamp_ = Long.MIN_VALUE;
		}

		if (this.configuredOptions_.hasOption("maxTimestamp")) {
			this.maxTimestamp_ = Long.parseLong(this.configuredOptions_
					.getOptionValue("maxTimestamp"));
		} else {
			this.maxTimestamp_ = Long.MAX_VALUE;
		}

		if (this.configuredOptions_.hasOption("timestamp")) {
			this.timestamp_ = Long.parseLong(this.configuredOptions_
					.getOptionValue("timestamp"));
		} else {
			this.timestamp_ = 0;
		}

		this.initScan(rowKey);
	}

	/**
	 * Returns UDFProperties based on <code>contextSignature</code>.
	 */
	private Properties getUDFProperties() {
		return UDFContext.getUDFContext().getUDFProperties(this.getClass(),
				new String[] { this.contextSignature });
	}

	/**
	 * @return <code> contextSignature + "_projectedFields" </code>
	 */
	private String projectedFieldsName() {
		return this.contextSignature + "_projectedFields";
	}

	/**
	 *
	 * @param columnList
	 * @param delimiter
	 * @param ignoreWhitespace
	 * @return
	 */
	private List<ColumnInfo> parseColumnList(final String columnList,
			final String delimiter, final boolean ignoreWhitespace) {
		final List<ColumnInfo> columnInfo = new ArrayList<ColumnInfo>();

		// Default behavior is to allow combinations of spaces and delimiter
		// which defaults to a comma. Setting to not ignore whitespace will
		// include the whitespace in the columns names
		String[] colNames = columnList.split(delimiter);
		if (ignoreWhitespace) {
			final List<String> columns = new ArrayList<String>();

			for (final String colName : colNames) {
				final String[] subColNames = colName.split(" ");

				for (String subColName : subColNames) {
					subColName = subColName.trim();
					if (subColName.length() > 0) {
						columns.add(subColName);
					}
				}
			}

			colNames = columns.toArray(new String[columns.size()]);
		}

		for (final String colName : colNames) {
			columnInfo.add(new ColumnInfo(colName));
		}

		return columnInfo;
	}

	private void initScan(final String rowKey) throws IOException {
		final byte[] bvector1 = this.readBloomfilterToByte(this.bitvectorPath1);
		byte[] bvector2 = "0".getBytes();
		if (this.bitvectorPath2 != null) {
			bvector2 = this.readBloomfilterToByte(this.bitvectorPath2);

		}

		this.scan = new Scan();

		if (this.bitvectorPath2 == null) {
			// scan.setFilter(new BitvectorFilter(bvector1));
		} else {
			// scan.setFilter(new BitvectorFilter(bvector1, bvector2));
		}

		// scan.setRaw(true);

		this.scan.setBatch(1);

		this.scan.setCaching(10000);

		if (rowKey != null) {
			this.scan.setStartRow(Bytes.toBytes(rowKey));
			// add random string because stopRow is exclusiv
			this.scan.setStopRow(Bytes.toBytes(rowKey + "z"));
		}

		// Map-reduce jobs should not run with cacheBlocks
		this.scan.setCacheBlocks(false);

		// Set filters, if any.
		if (this.configuredOptions_.hasOption("gt")) {
			this.gt_ = Bytes.toBytesBinary(Utils.slashisize(this.configuredOptions_
					.getOptionValue("gt")));
			this.addRowFilter(CompareOp.GREATER, this.gt_);
			this.scan.setStartRow(this.gt_);
		}
		if (this.configuredOptions_.hasOption("lt")) {
			this.lt_ = Bytes.toBytesBinary(Utils.slashisize(this.configuredOptions_
					.getOptionValue("lt")));
			this.addRowFilter(CompareOp.LESS, this.lt_);
			this.scan.setStopRow(this.lt_);
		}
		if (this.configuredOptions_.hasOption("gte")) {
			this.gte_ = Bytes.toBytesBinary(Utils.slashisize(this.configuredOptions_
					.getOptionValue("gte")));
			this.scan.setStartRow(this.gte_);
		}
		if (this.configuredOptions_.hasOption("lte")) {
			this.lte_ = Bytes.toBytesBinary(Utils.slashisize(this.configuredOptions_
					.getOptionValue("lte")));
			final byte[] lt = increment(this.lte_);
			if (LOG.isDebugEnabled()) {
				LOG.debug(String
						.format("Incrementing lte value of %s from bytes %s to %s to set stop row",
								Bytes.toString(this.lte_), toString(this.lte_),
								toString(lt)));
			}

			if (lt != null) {
				this.scan.setStopRow(increment(this.lte_));
			}

			// The WhileMatchFilter will short-circuit the scan after we no
			// longer match. The
			// setStopRow call will limit the number of regions we need to scan
			this.addFilter(new WhileMatchFilter(new RowFilter(
					CompareOp.LESS_OR_EQUAL, new BinaryComparator(this.lte_))));
		}
		if (this.configuredOptions_.hasOption("minTimestamp")
				|| this.configuredOptions_.hasOption("maxTimestamp")) {
			this.scan.setTimeRange(this.minTimestamp_, this.maxTimestamp_);
		}
		if (this.configuredOptions_.hasOption("timestamp")) {
			this.scan.setTimeStamp(this.timestamp_);
		}

		// if the group of columnInfos for this family doesn't contain a prefix,
		// we don't need
		// to set any filters, we can just call addColumn or addFamily. See
		// javadocs below.
		boolean columnPrefixExists = false;
		for (final ColumnInfo columnInfo : this.columnInfo_) {
			if (columnInfo.getColumnPrefix() != null) {
				columnPrefixExists = true;
				break;
			}
		}

		if (!columnPrefixExists) {
			this.addFiltersWithoutColumnPrefix(this.columnInfo_);
		} else {
			this.addFiltersWithColumnPrefix(this.columnInfo_);
		}
	}

	/**
	 * If there is no column with a prefix, we don't need filters, we can just
	 * call addColumn and addFamily on the scan
	 */
	private void addFiltersWithoutColumnPrefix(final List<ColumnInfo> columnInfos) {
		for (final ColumnInfo columnInfo : columnInfos) {
			if (columnInfo.columnName != null) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Adding column to scan via addColumn with cf:name = "
							+ Bytes.toString(columnInfo.getColumnFamily())
							+ ":" + Bytes.toString(columnInfo.getColumnName()));
				}
				this.scan.addColumn(columnInfo.getColumnFamily(),
						columnInfo.getColumnName());
			} else {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Adding column family to scan via addFamily with cf:name = "
							+ Bytes.toString(columnInfo.getColumnFamily()));
				}
				this.scan.addFamily(columnInfo.getColumnFamily());
			}
		}
	}

	/**
	 * If we have a qualifier with a prefix and a wildcard (i.e. cf:foo*), we
	 * need a filter on every possible column to be returned as shown below.
	 * This will become very inneficient for long lists of columns mixed with a
	 * prefixed wildcard.
	 *
	 * FilterList - must pass ALL of - FamilyFilter - AND a must pass ONE
	 * FilterList of - either Qualifier - or ColumnPrefixFilter
	 *
	 * If we have only column family filters (i.e. cf:*) or explicit column
	 * descriptors (i.e., cf:foo) or a mix of both then we don't need filters,
	 * since the scan will take care of that.
	 */
	private void addFiltersWithColumnPrefix(final List<ColumnInfo> columnInfos) {
		// we need to apply a CF AND column list filter for each family
		FilterList allColumnFilters = null;
		final Map<String, List<ColumnInfo>> groupedMap = groupByFamily(columnInfos);
		for (final String cfString : groupedMap.keySet()) {
			final List<ColumnInfo> columnInfoList = groupedMap.get(cfString);
			final byte[] cf = Bytes.toBytes(cfString);

			// all filters roll up to one parent OR filter
			if (allColumnFilters == null) {
				allColumnFilters = new FilterList(
						FilterList.Operator.MUST_PASS_ONE);
			}

			// each group contains a column family filter AND (all) and an OR
			// (one of) of
			// the column filters
			final FilterList thisColumnGroupFilter = new FilterList(
					FilterList.Operator.MUST_PASS_ALL);
			thisColumnGroupFilter.addFilter(new FamilyFilter(CompareOp.EQUAL,
					new BinaryComparator(cf)));
			final FilterList columnFilters = new FilterList(
					FilterList.Operator.MUST_PASS_ONE);
			for (final ColumnInfo colInfo : columnInfoList) {
				if (colInfo.isColumnMap()) {

					if (LOG.isDebugEnabled()) {
						LOG.debug("Adding family:prefix filters with values "
								+ Bytes.toString(colInfo.getColumnFamily())
								+ COLON
								+ Bytes.toString(colInfo.getColumnPrefix()));
					}

					// add a PrefixFilter to the list of column filters
					if (colInfo.getColumnPrefix() != null) {
						columnFilters.addFilter(new ColumnPrefixFilter(colInfo
								.getColumnPrefix()));
					}
				} else {

					if (LOG.isDebugEnabled()) {
						LOG.debug("Adding family:descriptor filters with values "
								+ Bytes.toString(colInfo.getColumnFamily())
								+ COLON
								+ Bytes.toString(colInfo.getColumnName()));
					}

					// add a QualifierFilter to the list of column filters
					columnFilters.addFilter(new QualifierFilter(
							CompareOp.EQUAL, new BinaryComparator(colInfo
									.getColumnName())));
				}
			}
			thisColumnGroupFilter.addFilter(columnFilters);
			allColumnFilters.addFilter(thisColumnGroupFilter);
		}
		if (allColumnFilters != null) {
			this.addFilter(allColumnFilters);
		}
	}

	private void addRowFilter(final CompareOp op, final byte[] val) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Adding filter " + op.toString() + " with value "
					+ Bytes.toStringBinary(val));
		}
		this.addFilter(new RowFilter(op, new BinaryComparator(val)));
	}

	private void addFilter(final Filter filter) {
		FilterList scanFilter = (FilterList) this.scan.getFilter();
		if (scanFilter == null) {
			scanFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		}
		scanFilter.addFilter(filter);
		this.scan.setFilter(scanFilter);
	}

	/**
	 * Returns the ColumnInfo list for so external objects can inspect it. This
	 * is available for unit testing. Ideally, the unit tests and the main
	 * source would each mirror the same package structure and this method could
	 * be package private.
	 *
	 * @return ColumnInfo
	 */
	public List<ColumnInfo> getColumnInfoList() {
		return this.columnInfo_;
	}

	@Override
	public Tuple getNext() throws IOException {
		try {
			// if (bitvector1 == null) {
			// bitvector1 = readBloomfilter(bitvectorPath1);
			// if (bitvectorPath2 != null) {
			// bitvector2 = readBloomfilter(bitvectorPath2);
			// }
			// }

			if (this.reader.nextKeyValue()) {
				final Result result = (Result) this.reader.getCurrentValue();

				Tuple tuple = null;

				// use a map of families -> qualifiers with the most recent
				// version of the cell. Fetching multiple vesions could be a
				// useful feature.
				final NavigableMap<byte[], NavigableMap<byte[], byte[]>> resultsMap = result
						.getNoVersionMap();

				final ArrayList<String> tupleList = new ArrayList<String>();

				if (this.loadRowKey_) {
					final ImmutableBytesWritable rowKey = (ImmutableBytesWritable) this.reader
							.getCurrentKey();
					tupleList.add(Bytes.toString(rowKey.get()));
				}

				for (int i = 0; i < this.columnInfo_.size(); ++i) {
					// int currentIndex = startIndex + i;

					final ColumnInfo columnInfo = this.columnInfo_.get(i);
					if (columnInfo.isColumnMap()) {
						// It's a column family so we need to iterate and set
						// all
						// values found
						final NavigableMap<byte[], byte[]> cfResults = resultsMap
								.get(columnInfo.getColumnFamily());
						// Map<String, DataByteArray> cfMap = new
						// HashMap<String, DataByteArray>();
						// Map<String, DataByteArray> cfMap = new
						// LinkedHashMap<String, DataByteArray>();
						if (cfResults != null) {
							for (final byte[] quantifier : cfResults.keySet()) {
								// We need to check against the prefix filter to
								// see if this value should be included. We
								// can't
								// just rely on the server-side filter, since a
								// user could specify multiple CF filters for
								// the
								// same CF.
								if (columnInfo.getColumnPrefix() == null
										|| columnInfo
												.hasPrefixMatch(quantifier)) {
									final String toSplit = Bytes.toString(quantifier);
									if (toSplit.contains(",")) {
										// boolean element1IsNecessary = true;
										// boolean element2IsNecessary = true;
										// 1
										final String toAdd1 = toSplit.substring(0,
												toSplit.indexOf(","));
										// if (bitvector1 != null
										// && !isElementPartOfBitvector(
										// toAdd1, bitvector1)) {
										// element1IsNecessary = false;
										// return TupleFactory.getInstance()
										// .newTuple(tupleList.size());
										// }
										tupleList.add(toAdd1);

										// 2
										final String toAdd2 = toSplit.substring(
												toSplit.indexOf(",") + 1,
												toSplit.length());

										// if (bitvector2 != null
										// && !isElementPartOfBitvector(
										// toAdd2, bitvector2)) {
										// // element2IsNecessary = false;
										// return TupleFactory.getInstance()
										// .newTuple(tupleList.size());
										// }
										tupleList.add(toAdd2);

										// if (!element1IsNecessary ||
										// !element2IsNecessary) {
										// // return null;
										// return
										// TupleFactory.getInstance().newTuple(0);
										// }
									} else {
										final String toAdd = Bytes
												.toString(quantifier);
										// if (bitvector1 != null
										// && !isElementPartOfBitvector(
										// toAdd, bitvector1)) {
										// // return null;
										// return TupleFactory.getInstance()
										// .newTuple(tupleList.size());
										// }
										tupleList.add(toAdd);
									}

								}
							}
						}
						tuple = TupleFactory.getInstance().newTuple(
								tupleList.size());
						int tuplePos = 0;
						for (final String elem : tupleList) {
							tuple.set(tuplePos, elem);
							tuplePos++;
						}
					} else {
						// kommt nicht vor
						// It's a column so set the value
						// byte[] cell = result.getValue(
						// columnInfo.getColumnFamily(),
						// columnInfo.getColumnName());
						// DataByteArray value = cell == null ? null
						// : new DataByteArray(cell);
						// tuple.set(currentIndex, value);
					}
				}

				// if (LOG.isDebugEnabled()) {
				// for (int i = 0; i < tuple.size(); i++) {
				// LOG.debug("tuple value:" + tuple.get(i));
				// }
				// }

				return tuple;
			}
		} catch (final InterruptedException e) {
			throw new IOException(e);
		}
		return null;
	}

	@Deprecated
	private boolean isElementPartOfBitvector(final String element, final BitSet bitvector) {
		final Integer position = BitvectorManager.hash(element.getBytes());
		if (bitvector.get(position)) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	public InputFormat getInputFormat() {
		final TableInputFormat inputFormat = new HBaseTableIFBuilder()
				.withLimit(this.limit_).withGt(this.gt_).withGte(this.gte_).withLt(this.lt_)
				.withLte(this.lte_).withConf(this.m_conf).build();
		return inputFormat;
	}

	@Override
	public void prepareToRead(final RecordReader reader, final PigSplit split) {
		this.reader = reader;
	}

	@Override
	public void setUDFContextSignature(final String signature) {
		this.contextSignature = signature;
	}

	@Override
	public void setLocation(final String location, final Job job) throws IOException {
		final Properties udfProps = this.getUDFProperties();
		job.getConfiguration().setBoolean("pig.noSplitCombination", true);

		this.initialiseHBaseClassLoaderResources(job);
		this.m_conf = this.initializeLocalJobConfig(job);
		final String delegationTokenSet = udfProps.getProperty(HBASE_TOKEN_SET);
		if (delegationTokenSet == null) {
			this.addHBaseDelegationToken(this.m_conf, job);
			udfProps.setProperty(HBASE_TOKEN_SET, "true");
		}

		String tablename = location;
		if (location.startsWith("hbase://")) {
			tablename = location.substring(8);
		}
		if (this.m_table == null) {
			this.m_table = new HTable(this.m_conf, tablename);
		}
		this.m_table.setScannerCaching(this.caching_);
		this.m_conf.set(TableInputFormat.INPUT_TABLE, tablename);

		final String projectedFields = udfProps.getProperty(this.projectedFieldsName());
		if (projectedFields != null) {
			// update columnInfo_
			this.pushProjection((RequiredFieldList) ObjectSerializer
					.deserialize(projectedFields));
		}

		for (final ColumnInfo columnInfo : this.columnInfo_) {
			// do we have a column family, or a column?
			if (columnInfo.isColumnMap()) {
				this.scan.addFamily(columnInfo.getColumnFamily());
			} else {
				this.scan.addColumn(columnInfo.getColumnFamily(),
						columnInfo.getColumnName());
			}

		}
		if (this.requiredFieldList != null) {
			final Properties p = UDFContext.getUDFContext().getUDFProperties(
					this.getClass(), new String[] { this.contextSignature });
			p.setProperty(this.contextSignature + "_projectedFields",
					ObjectSerializer.serialize(this.requiredFieldList));
		}
		this.m_conf.set(TableInputFormat.SCAN, convertScanToString(this.scan));
	}

	private void initialiseHBaseClassLoaderResources(final Job job)
			throws IOException {
		// Make sure the HBase, ZooKeeper, and Guava jars get shipped.
		TableMapReduceUtil.addDependencyJars(job.getConfiguration(),
				org.apache.hadoop.hbase.client.HTable.class,
				com.google.common.collect.Lists.class,
				org.apache.zookeeper.ZooKeeper.class);

	}

	private JobConf initializeLocalJobConfig(final Job job) {
		final Properties udfProps = this.getUDFProperties();
		final Configuration jobConf = job.getConfiguration();
		final JobConf localConf = new JobConf(jobConf);
		if (udfProps.containsKey(HBASE_CONFIG_SET)) {
			for (final Entry<Object, Object> entry : udfProps.entrySet()) {
				localConf.set((String) entry.getKey(),
						(String) entry.getValue());
			}
		} else {
			final Configuration hbaseConf = HBaseConfiguration.create();
			for (final Entry<String, String> entry : hbaseConf) {
				// JobConf may have some conf overriding ones in hbase-site.xml
				// So only copy hbase config not in job config to UDFContext
				// Also avoids copying core-default.xml and core-site.xml
				// props in hbaseConf to UDFContext which would be redundant.
				if (jobConf.get(entry.getKey()) == null) {
					udfProps.setProperty(entry.getKey(), entry.getValue());
					localConf.set(entry.getKey(), entry.getValue());
				}
			}
			udfProps.setProperty(HBASE_CONFIG_SET, "true");
		}
		return localConf;
	}

	/**
	 * Get delegation token from hbase and add it to the Job
	 *
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void addHBaseDelegationToken(final Configuration hbaseConf, final Job job) {

		if (!UDFContext.getUDFContext().isFrontend()) {
			return;
		}

		if ("kerberos".equalsIgnoreCase(hbaseConf.get(HBASE_SECURITY_CONF_KEY))) {
			// Will not be entering this block for 0.20.2 as it has no security.
			try {
				// getCurrentUser method is not public in 0.20.2
				final Method m1 = UserGroupInformation.class
						.getMethod("getCurrentUser");
				final UserGroupInformation currentUser = (UserGroupInformation) m1
						.invoke(null, (Object[]) null);
				// hasKerberosCredentials method not available in 0.20.2
				final Method m2 = UserGroupInformation.class
						.getMethod("hasKerberosCredentials");
				final boolean hasKerberosCredentials = (Boolean) m2.invoke(
						currentUser, (Object[]) null);
				if (hasKerberosCredentials) {
					// Class and method are available only from 0.92 security
					// release
					final Class tokenUtilClass = Class
							.forName("org.apache.hadoop.hbase.security.token.TokenUtil");
					final Method m3 = tokenUtilClass.getMethod("obtainTokenForJob",
							new Class[] { Configuration.class,
									UserGroupInformation.class, Job.class });
					m3.invoke(null,
							new Object[] { hbaseConf, currentUser, job });
				} else {
					LOG.info("Not fetching hbase delegation token as no Kerberos TGT is available");
				}
			} catch (final ClassNotFoundException cnfe) {
				throw new RuntimeException("Failure loading TokenUtil class, "
						+ "is secure RPC available?", cnfe);
			} catch (final RuntimeException re) {
				throw re;
			} catch (final Exception e) {
				throw new UndeclaredThrowableException(e,
						"Unexpected error calling TokenUtil.obtainTokenForJob()");
			}
		}
	}

	@Override
	public String relativeToAbsolutePath(final String location, final Path curDir)
			throws IOException {
		return location;
	}

	private static String convertScanToString(final Scan scan) {
		try {
			final ByteArrayOutputStream out = new ByteArrayOutputStream();
			final DataOutputStream dos = new DataOutputStream(out);
			scan.write(dos);
			return Base64.encodeBytes(out.toByteArray());
		} catch (final IOException e) {
			LOG.error(e);
			return "";
		}

	}

	/**
	 * Set up the caster to use for reading values out of, and writing to,
	 * HBase.
	 */
	@Override
	public LoadCaster getLoadCaster() throws IOException {
		return this.caster_;
	}

	/*
	 * StoreFunc Methods
	 *
	 * @see org.apache.pig.StoreFuncInterface#getOutputFormat()
	 */

	@Override
	public OutputFormat getOutputFormat() throws IOException {
		if (this.outputFormat == null) {
			if (this.m_conf == null) {
				throw new IllegalStateException(
						"setStoreLocation has not been called");
			} else {
				this.outputFormat = new TableOutputFormat();
				this.outputFormat.setConf(this.m_conf);
			}
		}
		return this.outputFormat;
	}

	@Override
	public void checkSchema(final ResourceSchema s) throws IOException {
		if (!(this.caster_ instanceof LoadStoreCaster)) {
			LOG.error("Caster must implement LoadStoreCaster for writing to HBase.");
			throw new IOException("Bad Caster " + this.caster_.getClass());
		}
		this.schema_ = s;
		this.getUDFProperties().setProperty(this.contextSignature + "_schema",
				ObjectSerializer.serialize(this.schema_));
	}

	// Suppressing unchecked warnings for RecordWriter, which is not
	// parameterized by StoreFuncInterface
	@Override
	public void prepareToWrite(@SuppressWarnings("rawtypes") final RecordWriter writer)
			throws IOException {
		this.writer = writer;
	}

	// Suppressing unchecked warnings for RecordWriter, which is not
	// parameterized by StoreFuncInterface
	@SuppressWarnings("unchecked")
	@Override
	public void putNext(final Tuple t) throws IOException {
		final ResourceFieldSchema[] fieldSchemas = (this.schema_ == null) ? null : this.schema_
				.getFields();
		final byte type = (fieldSchemas == null) ? DataType.findType(t.get(0))
				: fieldSchemas[0].getType();
		final long ts = System.currentTimeMillis();

		final Put put = this.createPut(t.get(0), type);

		if (LOG.isDebugEnabled()) {
			LOG.debug("putNext -- WAL disabled: " + this.noWAL_);
			for (final ColumnInfo columnInfo : this.columnInfo_) {
				LOG.debug("putNext -- col: " + columnInfo);
			}
		}

		for (int i = 1; i < t.size(); ++i) {
			final ColumnInfo columnInfo = this.columnInfo_.get(i - 1);
			if (LOG.isDebugEnabled()) {
				LOG.debug("putNext - tuple: " + i + ", value=" + t.get(i)
						+ ", cf:column=" + columnInfo);
			}

			if (!columnInfo.isColumnMap()) {
				put.add(columnInfo.getColumnFamily(),
						columnInfo.getColumnName(),
						ts,
						this.objToBytes(
								t.get(i),
								(fieldSchemas == null) ? DataType.findType(t
										.get(i)) : fieldSchemas[i].getType()));
			} else {
				final Map<String, Object> cfMap = (Map<String, Object>) t.get(i);
				for (final String colName : cfMap.keySet()) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("putNext - colName=" + colName + ", class: "
								+ colName.getClass());
					}
					// TODO deal with the fact that maps can have types now.
					// Currently we detect types at
					// runtime in the case of storing to a cf, which is
					// suboptimal.
					put.add(columnInfo.getColumnFamily(),
							Bytes.toBytes(colName.toString()),
							ts,
							this.objToBytes(cfMap.get(colName),
									DataType.findType(cfMap.get(colName))));
				}
			}
		}

		try {
			this.writer.write(null, put);
		} catch (final InterruptedException e) {
			throw new IOException(e);
		}
	}

	/**
	 * Public method to initialize a Put. Used to allow assertions of how Puts
	 * are initialized by unit tests.
	 *
	 * @param key
	 * @param type
	 * @return new put
	 * @throws IOException
	 */
	public Put createPut(final Object key, final byte type) throws IOException {
		final Put put = new Put(this.objToBytes(key, type));

		if (this.noWAL_) {
			put.setWriteToWAL(false);
		}

		return put;
	}

	@SuppressWarnings("unchecked")
	private byte[] objToBytes(final Object o, final byte type) throws IOException {
		final LoadStoreCaster caster = (LoadStoreCaster) this.caster_;
		if (o == null) {
			return null;
		}
		switch (type) {
		case DataType.BYTEARRAY:
			return ((DataByteArray) o).get();
		case DataType.BAG:
			return caster.toBytes((DataBag) o);
		case DataType.CHARARRAY:
			return caster.toBytes((String) o);
		case DataType.DOUBLE:
			return caster.toBytes((Double) o);
		case DataType.FLOAT:
			return caster.toBytes((Float) o);
		case DataType.INTEGER:
			return caster.toBytes((Integer) o);
		case DataType.LONG:
			return caster.toBytes((Long) o);
		case DataType.BOOLEAN:
			return caster.toBytes((Boolean) o);
		case DataType.DATETIME:
			return caster.toBytes((DateTime) o);

			// The type conversion here is unchecked.
			// Relying on DataType.findType to do the right thing.
		case DataType.MAP:
			return caster.toBytes((Map<String, Object>) o);

		case DataType.NULL:
			return null;
		case DataType.TUPLE:
			return caster.toBytes((Tuple) o);
		case DataType.ERROR:
			throw new IOException("Unable to determine type of " + o.getClass());
		default:
			throw new IOException("Unable to find a converter for tuple field "
					+ o);
		}
	}

	@Override
	public String relToAbsPathForStoreLocation(final String location, final Path curDir)
			throws IOException {
		return location;
	}

	@Override
	public void setStoreFuncUDFContextSignature(final String signature) {
		this.contextSignature = signature;
	}

	@Override
	public void setStoreLocation(final String location, final Job job) throws IOException {
		if (location.startsWith("hbase://")) {
			job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE,
					location.substring(8));
		} else {
			job.getConfiguration()
					.set(TableOutputFormat.OUTPUT_TABLE, location);
		}

		final String serializedSchema = this.getUDFProperties().getProperty(
				this.contextSignature + "_schema");
		if (serializedSchema != null) {
			this.schema_ = (ResourceSchema) ObjectSerializer
					.deserialize(serializedSchema);
		}

		this.initialiseHBaseClassLoaderResources(job);
		this.m_conf = this.initializeLocalJobConfig(job);
		// Not setting a udf property and getting the hbase delegation token
		// only once like in setLocation as setStoreLocation gets different Job
		// objects for each call and the last Job passed is the one that is
		// launched. So we end up getting multiple hbase delegation tokens.
		this.addHBaseDelegationToken(this.m_conf, job);
	}

	@Override
	public void cleanupOnFailure(final String location, final Job job) throws IOException {
	}

	@Override
	public void cleanupOnSuccess(final String location, final Job job) throws IOException {
	}

	/*
	 * LoadPushDown Methods.
	 */

	@Override
	public List<OperatorSet> getFeatures() {
		return Arrays.asList(LoadPushDown.OperatorSet.PROJECTION);
	}

	@Override
	public RequiredFieldResponse pushProjection(
			final RequiredFieldList requiredFieldList) throws FrontendException {
		final List<RequiredField> requiredFields = requiredFieldList.getFields();
		final List<ColumnInfo> newColumns = Lists
				.newArrayListWithExpectedSize(requiredFields.size());

		if (this.requiredFieldList != null) {
			// in addition to PIG, this is also called by this.setLocation().
			LOG.debug("projection is already set. skipping.");
			return new RequiredFieldResponse(true);
		}

		/*
		 * How projection is handled : - pushProjection() is invoked by PIG on
		 * the front end - pushProjection here both stores serialized projection
		 * in the context and adjusts columnInfo_. - setLocation() is invoked on
		 * the backend and it reads the projection from context. setLocation
		 * invokes this method again so that columnInfo_ is adjected.
		 */

		// colOffset is the offset in our columnList that we need to apply to
		// indexes we get from requiredFields
		// (row key is not a real column)
		final int colOffset = this.loadRowKey_ ? 1 : 0;
		// projOffset is the offset to the requiredFieldList we need to apply
		// when figuring out which columns to prune.
		// (if key is pruned, we should skip row key's element in this list when
		// trimming colList)
		int projOffset = colOffset;
		this.requiredFieldList = requiredFieldList;

		if (requiredFieldList != null
				&& requiredFields.size() > (this.columnInfo_.size() + colOffset)) {
			throw new FrontendException(
					"The list of columns to project from HBase is larger than HBaseStorage is configured to load.");
		}

		// remember the projection
		try {
			this.getUDFProperties().setProperty(this.projectedFieldsName(),
					ObjectSerializer.serialize(requiredFieldList));
		} catch (final IOException e) {
			throw new FrontendException(e);
		}

		if (this.loadRowKey_
				&& (requiredFields.size() < 1 || requiredFields.get(0)
						.getIndex() != 0)) {
			this.loadRowKey_ = false;
			projOffset = 0;
		}

		for (int i = projOffset; i < requiredFields.size(); i++) {
			final int fieldIndex = requiredFields.get(i).getIndex();
			newColumns.add(this.columnInfo_.get(fieldIndex - colOffset));
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("pushProjection After Projection: loadRowKey is "
					+ this.loadRowKey_);
			for (final ColumnInfo colInfo : newColumns) {
				LOG.debug("pushProjection -- col: " + colInfo);
			}
		}
		this.columnInfo_ = newColumns;
		return new RequiredFieldResponse(true);
	}

	// old buggy code
	// @Override
	// public WritableComparable<InputSplit> getSplitComparable(InputSplit
	// split)
	// throws IOException {
	// return new WritableComparable<InputSplit>() {
	// TableSplit tsplit = new TableSplit();
	//
	// @Override
	// public void readFields(DataInput in) throws IOException {
	// tsplit.readFields(in);
	// }
	//
	// @Override
	// public void write(DataOutput out) throws IOException {
	// tsplit.write(out);
	// }
	//
	// @Override
	// public int compareTo(InputSplit split) {
	// return tsplit.compareTo((TableSplit) split);
	// }
	// };
	// }

	public WritableComparable<TableSplit> getSplitComparable(final InputSplit split)
			throws IOException {
		if (split instanceof TableSplit) {
			return new TableSplitComparable((TableSplit) split);
		} else {
			throw new RuntimeException(
					"LoadFunc expected split of type TableSplit");
		}
	}

	/**
	 * Class to encapsulate logic around which column names were specified in
	 * each position of the column list. Users can specify columns names in one
	 * of 4 ways: 'Foo:', 'Foo:*', 'Foo:bar*' or 'Foo:bar'. The first 3 result
	 * in a Map being added to the tuple, while the last results in a scalar.
	 * The 3rd form results in a prefix-filtered Map.
	 */
	public class ColumnInfo {

		final String originalColumnName; // always set
		final byte[] columnFamily; // always set
		final byte[] columnName; // set if it exists and doesn't contain '*'
		final byte[] columnPrefix; // set if contains a prefix followed by '*'

		public ColumnInfo(final String colName) {
			this.originalColumnName = colName;
			final String[] cfAndColumn = colName.split(COLON, 2);

			// CFs are byte[1] and columns are byte[2]
			this.columnFamily = Bytes.toBytes(cfAndColumn[0]);
			if (cfAndColumn.length > 1 && cfAndColumn[1].length() > 0
					&& !ASTERISK.equals(cfAndColumn[1])) {
				if (cfAndColumn[1].endsWith(ASTERISK)) {
					this.columnPrefix = Bytes.toBytes(cfAndColumn[1].substring(0,
							cfAndColumn[1].length() - 1));
					this.columnName = null;
				} else {
					this.columnName = Bytes.toBytes(cfAndColumn[1]);
					this.columnPrefix = null;
				}
			} else {
				this.columnPrefix = null;
				this.columnName = null;
			}
		}

		public byte[] getColumnFamily() {
			return this.columnFamily;
		}

		public byte[] getColumnName() {
			return this.columnName;
		}

		public byte[] getColumnPrefix() {
			return this.columnPrefix;
		}

		public boolean isColumnMap() {
			return this.columnName == null;
		}

		public boolean hasPrefixMatch(final byte[] qualifier) {
			return Bytes.startsWith(qualifier, this.columnPrefix);
		}

		@Override
		public String toString() {
			return this.originalColumnName;
		}
	}

	/**
	 * Group the list of ColumnInfo objects by their column family and returns a
	 * map of CF to its list of ColumnInfo objects. Using String as key since it
	 * implements Comparable.
	 *
	 * @param columnInfos
	 *            the columnInfo list to group
	 * @return a Map of lists, keyed by their column family.
	 */
	static Map<String, List<ColumnInfo>> groupByFamily(
			final List<ColumnInfo> columnInfos) {
		final Map<String, List<ColumnInfo>> groupedMap = new HashMap<String, List<ColumnInfo>>();
		for (final ColumnInfo columnInfo : columnInfos) {
			final String cf = Bytes.toString(columnInfo.getColumnFamily());
			List<ColumnInfo> columnInfoList = groupedMap.get(cf);
			if (columnInfoList == null) {
				columnInfoList = new ArrayList<ColumnInfo>();
			}
			columnInfoList.add(columnInfo);
			groupedMap.put(cf, columnInfoList);
		}
		return groupedMap;
	}

	static String toString(final byte[] bytes) {
		if (bytes == null) {
			return null;
		}

		final StringBuffer sb = new StringBuffer();
		for (int i = 0; i < bytes.length; i++) {
			if (i > 0) {
				sb.append("|");
			}
			sb.append(bytes[i]);
		}
		return sb.toString();
	}

	/**
	 * Increments the byte array by one for use with setting stopRow. If all
	 * bytes in the array are set to the maximum byte value, then the original
	 * array will be returned with a 0 byte appended to it. This is because
	 * HBase compares bytes from left to right. If byte array B is equal to byte
	 * array A, but with an extra byte appended, A will be < B. For example
	 * {@code}A = byte[] {-1}{@code} increments to {@code}B = byte[] {-1, 0}
	 * {@code} and {@code}A < B{@code}
	 *
	 * @param bytes
	 *            array to increment bytes on
	 * @return a copy of the byte array incremented by 1
	 */
	static byte[] increment(final byte[] bytes) {
		boolean allAtMax = true;
		for (int i = 0; i < bytes.length; i++) {
			if ((bytes[bytes.length - i - 1] & 0x0ff) != 255) {
				allAtMax = false;
				break;
			}
		}

		if (allAtMax) {
			return Arrays.copyOf(bytes, bytes.length + 1);
		}

		final byte[] incremented = bytes.clone();
		for (int i = bytes.length - 1; i >= 0; i--) {
			boolean carry = false;
			final int val = bytes[i] & 0x0ff;
			int total = val + 1;
			if (total > 255) {
				carry = true;
				total %= 256;
			} else if (total < 0) {
				carry = true;
			}
			incremented[i] = (byte) total;
			if (!carry) {
				return incremented;
			}
		}
		return incremented;
	}
}
