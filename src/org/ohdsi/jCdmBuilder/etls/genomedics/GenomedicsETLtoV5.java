/*******************************************************************************
 * Copyright 2015 Observational Health Data Sciences and Informatics
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.ohdsi.jCdmBuilder.etls.genomedics;

import java.io.File;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
//import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
//import java.util.concurrent.TimeUnit;

import org.ohdsi.databases.DbType;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.jCdmBuilder.DbSettings;
import org.ohdsi.jCdmBuilder.EtlReport;
//import org.ohdsi.jCdmBuilder.ObjectExchange;
import org.ohdsi.jCdmBuilder.cdm.CdmV5NullableChecker;
//import org.ohdsi.jCdmBuilder.etls.genomedics.ObjectSizeFetcher;
//import org.ohdsi.jCdmBuilder.etls.genomedics.CodeFmcToDurgMap.CodeData;
import org.ohdsi.jCdmBuilder.utilities.CodeToConceptMap;
//import org.ohdsi.jCdmBuilder.utilities.CSVFileChecker;
//import org.ohdsi.jCdmBuilder.utilities.CodeToConceptMap;
import org.ohdsi.jCdmBuilder.utilities.CodeToDomainConceptMap;
//import org.ohdsi.jCdmBuilder.utilities.ETLUtils;
import org.ohdsi.jCdmBuilder.utilities.QCSampleConstructor;
import org.ohdsi.jCdmBuilder.utilities.CodeToDomainConceptMap.CodeDomainData;
import org.ohdsi.jCdmBuilder.utilities.CodeToDomainConceptMap.TargetConcept;
//import org.ohdsi.jCdmBuilder.vocabulary.InsertVocabularyInServer;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.collections.OneToManyList;
import org.ohdsi.utilities.files.ReadCSVFileWithHeader;
//import org.ohdsi.utilities.collections.RowUtilities;
//import org.ohdsi.utilities.files.FileSorter;
//import org.ohdsi.utilities.files.MultiRowIterator;
//import org.ohdsi.utilities.files.MultiRowIterator.MultiRowSet;
//import org.ohdsi.utilities.files.ReadCSVFileWithHeader;
import org.ohdsi.utilities.files.Row;
import org.ohdsi.utilities.files.WriteTextFile;
import org.ohdsi.utilities.files.IniFile;
//import org.ohdsi.utilities.files.WriteTextFile;

public class GenomedicsETLtoV5 {

	public static String				PAZIENTI_TABLE					= "pazienti";
	public static String				PAZPBL_TABLE					= "cart_pazpbl";
	public static String				TERAP_TABLE						= "cart_terap";
	public static String				ACCERT_TABLE					= "cart_accert";
	public static String				PRESS_TABLE						= "cart_press";
	public static String				DESCRIZ_TABLE					= "cart_descriz";
	public static String				RIABIL_TABLE					= "cart_riabil";
	public static String				VISITE_TABLE					= "cart_visite";

	public static String				GPID_SOURCE_FIELD				= "id";

	public static int					MAX_ROWS_PER_PERSON				= 100000;
	public static String[]				TABLES							= new String[] { PAZIENTI_TABLE, PAZPBL_TABLE, TERAP_TABLE, 
			ACCERT_TABLE, PRESS_TABLE, DESCRIZ_TABLE, RIABIL_TABLE, VISITE_TABLE };
	public static long					BATCH_SIZE_DEFAULT				= 1000;
	public static String[]				tablesInsideOP					= new String[] { "condition_occurrence", "procedure_occurrence", 
			"drug_exposure", "death", "visit_occurrence", "observation", "measurement"					};
	public static String[]				fieldInTables					= new String[] { "condition_start_date", "procedure_date", 
			"drug_exposure_start_date", "death_date", "visit_start_date", "observation_date", "measurement_date" };
	public static boolean				USE_VERBOSE_SOURCE_VALUES		= false;
	private static long					MINIMUM_FREE_MEMORY_DEFAULT		= 10000000;
	public static boolean				GENERATE_QASAMPLES_DEFAULT		= false;
	public static double				QA_SAMPLE_PROBABILITY_DEFAULT	= 0.0001;

	private String						folder;
	private RichConnection				sourceConnection;
	private RichConnection				targetConnection;
	private long						personId;
	private long						observationPeriodId;
	private long						drugExposureId;
	private long						drugCostId;
	private long						conditionOccurrenceId;
	private long						visitOccurrenceId;
	private long						procedureOccurrenceId;
	private	long						procedureCostId;
	private long						deviceExposureId;
	private long						deviceCostId;
	private long						locationId;
	private long						providerId;
	private long						observationId;
	private long						measurementId;
	private long						noteId;

	private OneToManyList<String, Row>	tableToRows;
	private CodeToConceptMap			umdToConcept;
	private CodeToConceptMap			viaToConcept;
	private CodeToDomainConceptMap		hsCodeToConcept;
	private CodeToDomainConceptMap		atcToConcept;
	private CodeToDomainConceptMap		icd9ToConcept;
	private CodeToDomainConceptMap		icd9ProcToConcept;
	private CodeFmcToDurgMap			codifaToDurgDDD;
	private Map<String, Long>			codiceToPatientId				= new HashMap<String, Long>();
	private Map<String, Long>			codiceToProviderId				= new HashMap<String, Long>();
	private Map<String, Long>			gpidToProviderId				= new HashMap<String, Long>();
	private Map<String, Long>			provinciaToLocationId			= new HashMap<String, Long>();
	private Map<String, Long>			codicePbToConditionOccurrenceId	= new HashMap<String, Long>();
	private QCSampleConstructor			qcSampleConstructor;
	private EtlReport					etlReport;
	private CdmV5NullableChecker		cdmv5NullableChecker			= new CdmV5NullableChecker();

	private IniFile						settings;
	private long						batchSize;			// How many patient records, and all related records, should be loaded before inserting batch in target
	private long						minimumFreeMemory;	// Monitor memory usage - if free memory falls below threshold, insert batch of records already loaded
	private boolean						useBatchInserts = true;
	public Boolean						generateQaSamples;		// Generate QA Samples at end of ETL
	public double						qaSampleProbability;	// The sample probability value used to include a patient's records in the QA Sample export

	private String						logFilename;
	private WriteTextFile 				logOut = null;
	private DecimalFormat 				df = new DecimalFormat();
	private DecimalFormatSymbols 		dfs = new DecimalFormatSymbols();

	// ******************** Process ********************//
	public void process(String folder, DbSettings sourceDbSettings, DbSettings targetDbSettings, int maxPersons, int versionId) {
		StringUtilities.outputWithTime("OS name : "+System.getProperty("os.name"));
		StringUtilities.outputWithTime("OS arch : "+System.getProperty("os.arch"));
		loadMappings(targetDbSettings);
		loadSourceMappings(sourceDbSettings);
		loadSettings();

		sourceConnection = new RichConnection(sourceDbSettings.server, sourceDbSettings.domain, sourceDbSettings.user, sourceDbSettings.password,
				sourceDbSettings.dbType);
		sourceConnection.setContext(this.getClass());
		sourceConnection.use(sourceDbSettings.database);

		targetConnection = new RichConnection(targetDbSettings.server, targetDbSettings.domain, targetDbSettings.user, targetDbSettings.password,
				targetDbSettings.dbType);
		targetConnection.setContext(this.getClass());
		targetConnection.use(targetDbSettings.database);
		List<String> tableNames = targetConnection.getTableNames(targetDbSettings.database);

		if (!tableNames.contains("cost"))
			createCostTable(targetDbSettings);
		if (!tableNames.contains("_version"))
			targetConnection.execute("CREATE TABLE _version(version_id INT,version_date DATE);");	
		
		truncateTables(targetConnection);
		this.folder = folder;

		String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
		targetConnection.execute("INSERT INTO _version (version_id, version_date) VALUES (" + versionId + ", '" + date + "')");
		
		personId = 0;
		observationPeriodId = 0;
		drugExposureId = 0;
		drugCostId = 0;
		conditionOccurrenceId = 0;
		visitOccurrenceId = 0;
		procedureOccurrenceId = 0;
		procedureCostId = 0;
		deviceExposureId = 0;
		deviceCostId = 0;
		observationId = 0;
		measurementId = 0;
		locationId = 0;
		providerId = 0;
		noteId = 0;
		codiceToPatientId.clear();
		codiceToProviderId.clear();
		gpidToProviderId.clear();
		provinciaToLocationId.clear();
		codicePbToConditionOccurrenceId.clear();

		if (generateQaSamples)
			qcSampleConstructor = new QCSampleConstructor(folder + "/sample", qaSampleProbability);
		tableToRows = new OneToManyList<String, Row>();
		etlReport = new EtlReport(folder);

		StringUtilities.outputWithTime("Populating CDM_Source table");
		populateCdmSourceTable();

		StringUtilities.outputWithTime("Processing persons");
		long personRecordsInBatch = 0;

		for (Row row : sourceConnection.query("SELECT * FROM "+ PAZIENTI_TABLE)) {
			if (useBatchInserts) {
				processPazientiSourceRecord(row);
				personRecordsInBatch++;

				if (personId == maxPersons) {
					StringUtilities.outputWithTime("Reached limit of " + maxPersons + " persons, terminating");
					break;
				}
				if ((personRecordsInBatch % batchSize == 0) | (memoryGettingLow())) {
					insertBatch();
					StringUtilities.outputWithTime("Processed " + personId + " persons");
					personRecordsInBatch = 0;
				}
			}
			else { // Don't use batch inserts - insert records per table as loaded
				processPazientiSourceRecord(row);
				if (personId == maxPersons) {
					StringUtilities.outputWithTime("Reached limit of " + maxPersons + " persons, terminating");
					break;
				}
			}
		}

		if (useBatchInserts)
			insertBatch(); // Insert (last) batch

		StringUtilities.outputWithTime("Processed " + personId + " persons");

		if (generateQaSamples)
			qcSampleConstructor.addCdmData(targetConnection, targetDbSettings.database);

		String etlReportName = etlReport.generateETLReport(icd9ToConcept, icd9ProcToConcept, atcToConcept);
		StringUtilities.outputWithTime("An ETL report was generated and written to :" + etlReportName);
		if (etlReport.getTotalProblemCount() > 0) {
			String etlProblemListname = etlReport.generateProblemReport();
			StringUtilities.outputWithTime("An ETL problem list was generated and written to :" + etlProblemListname);
		}

		StringUtilities.outputWithTime("Finished ETL");

		if (logOut != null)
			logOut.close();
	}

	//********************  ********************//
	private void setUpLogFile() {
		logFilename = folder + "/etlLog.txt";
		int i = 1;
		while (new File(logFilename).exists())
			logFilename = folder + "/etlLog" + (i++) + ".txt";
		dfs.setGroupingSeparator(',');
		df.setDecimalFormatSymbols(dfs);
		logOut = new WriteTextFile(logFilename);

		StringUtilities.outputWithTime("ETL log file created: "+ logFilename);
	}

	//********************  ********************//
	private void writeToLog(String message) {
		if (logOut == null)
			setUpLogFile();
		logOut.writeln(StringUtilities.now() + "\t" + message);

	}

	//********************  ********************//
	private long calculatedFreeMemory() {
		return Runtime.getRuntime().maxMemory() - (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
	}

	private static String addDaysToDate(String theDate, int days) {	 // String of format yyyy-MM-dd
		DateFormat dateFormat= new SimpleDateFormat("yyyy-MM-dd");
		Date baseDate = null;
		try {
			baseDate = dateFormat.parse(theDate);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		if (baseDate != null) {
			Calendar c = Calendar.getInstance();    
			c.setTime(baseDate);
			c.add(Calendar.DAY_OF_YEAR, days);
			return dateFormat.format(c.getTime());
		}
		else {
			return theDate;
		}
	}
	
	//********************  ********************//
	private void populateCdmSourceTable() {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		targetConnection.executeResource("sql/PopulateCdmSourceTable.sql", "@today", df.format(new Date()));
	}

	//********************  ********************//
	private void truncateTables(RichConnection targetConnection) {
		StringUtilities.outputWithTime("Truncating tables");
		String[] tables = new String[] { "attribute_definition", "care_site", "cdm_source", "cohort", "cohort_attribute", "cohort_definition", "condition_era",
				"condition_occurrence", "death", "cost", "device_exposure", "dose_era", "drug_era", "drug_exposure", "fact_relationship",
				"location", "measurement", "note", "observation", "observation_period", "payer_plan_period", "person",
				"procedure_occurrence", "provider", "specimen", "visit_occurrence" };
		for (String table : tables)
			targetConnection.execute("TRUNCATE TABLE " + table);	}

	//********************  ********************//
	private void insertBatch() {
		removeRowsWithNonNullableNulls();
		removeRowsOutsideOfObservationTime();

		etlReport.registerOutgoingData(tableToRows);
		for (String table : tableToRows.keySet())
			targetConnection.insertIntoTable(tableToRows.get(table).iterator(), table, false, true);
		tableToRows.clear();
	}

	//********************  ********************//
	private void loadMappings(DbSettings dbSettings) {
		StringUtilities.outputWithTime("Loading mappings from server");
		RichConnection targetConnection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		targetConnection.setContext(this.getClass());
		targetConnection.use(dbSettings.database);

		System.out.println("- Loading ATC to concept_id mapping");
		atcToConcept = new CodeToDomainConceptMap("ATC to concept_id mapping", "Drug");
		for (Row row : targetConnection.queryResource("sql/atcToRxNorm.sql")) {
			row.upperCaseFieldNames();
			atcToConcept.add(row.get("SOURCE_CODE"), row.get("SOURCE_NAME"), row.getInt("SOURCE_CONCEPT_ID"), row.getInt("TARGET_CONCEPT_ID"),
					row.get("TARGET_CODE"), row.get("TARGET_NAME"), row.get("DOMAIN_ID"));
		}

		System.out.println("- Loading ICD-9 to concept_id mapping");
		icd9ToConcept = new CodeToDomainConceptMap("ICD-9 to concept_id mapping", "Condition");
		for (Row row : targetConnection.queryResource("sql/icd9ToConditionProcMeasObsDevice.sql")) {
			row.upperCaseFieldNames();
			icd9ToConcept.add(row.get("SOURCE_CODE"), row.get("SOURCE_NAME"), row.getInt("SOURCE_CONCEPT_ID"), row.getInt("TARGET_CONCEPT_ID"),
					row.get("TARGET_CODE"), row.get("TARGET_NAME"), row.get("DOMAIN_ID"));
		}

		System.out.println("- Loading ICD-9 Procedure to concept_id mapping");
		icd9ProcToConcept = new CodeToDomainConceptMap("ICD-9 Procedure to concept_id mapping", "Procedure");
		for (Row row : targetConnection.queryResource("sql/icd9ProcToProcMeasObsDrugCondition.sql")) {
			row.upperCaseFieldNames();
			icd9ProcToConcept.add(row.get("SOURCE_CODE"), row.get("SOURCE_NAME"), row.getInt("SOURCE_CONCEPT_ID"), row.getInt("TARGET_CONCEPT_ID"),
					row.get("TARGET_CODE"), row.get("TARGET_NAME"), row.get("DOMAIN_ID"));
		}

		System.out.println("- Loading HS_CODE to concept_id mapping");
		hsCodeToConcept = new CodeToDomainConceptMap("HS_CODE to concept_id mapping", "Procedure");
		for (Row row : new ReadCSVFileWithHeader(this.getClass().getResourceAsStream("csv/HS_CODE.csv"))) {
			row.upperCaseFieldNames();
			hsCodeToConcept.add(row.get("HS_CODE"), row.get("MEANING"), 0, row.getInt("CONCEPT_ID"),
					row.get("CONCEPT_CODE"), row.get("CONCEPT_NAME"), row.get("DOMAIN_ID"));
		}

		System.out.println("- Loading UNITA_MIS_DDD to concept mapping");
		umdToConcept = new CodeToConceptMap("UNITA_MIS_DDD to concept mapping");
		for (Row row : new ReadCSVFileWithHeader(this.getClass().getResourceAsStream("csv/UNITA_MIS_DDD.csv"))) {
			row.upperCaseFieldNames();
			umdToConcept.add(row.get("UNITA_MIS_DDD"), row.get("MEANING"), row.getInt("CONCEPT_ID"), row.get("CONCEPT_CODE"),
					row.get("CONCEPT_NAME"));
		}

		System.out.println("- Loading VIA to concept mapping");
		viaToConcept = new CodeToConceptMap("VIA to concept mapping");
		for (Row row : new ReadCSVFileWithHeader(this.getClass().getResourceAsStream("csv/VIA.csv"))) {
			row.upperCaseFieldNames();
			viaToConcept.add(row.get("VIA"), row.get("MEANING"), row.getInt("CONCEPT_ID"), row.get("CONCEPT_CODE"),
					row.get("CONCEPT_NAME"));
		}

		StringUtilities.outputWithTime("Finished loading mappings");
	}

	//********************  ********************//
	private void loadSourceMappings(DbSettings dbSettings) {
		StringUtilities.outputWithTime("Loading mappings from source server");
		RichConnection targetConnection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		targetConnection.setContext(this.getClass());
		targetConnection.use(dbSettings.database);

		System.out.println("- Loading codifa to DURG_DDD mapping");
		codifaToDurgDDD = new CodeFmcToDurgMap("Specialty to concept mapping");
		for (Row row : targetConnection.queryResource("sql/codifaToDurgDdd.sql")) {
			row.upperCaseFieldNames();
			codifaToDurgDDD.add(row.get("COD_FARMACO"), row.get("DESCRIZIONE"), row.get("COD_ATC5"), row.get("VIA"), 
					getNumValue(row.get("QUANT_PA")), getNumValue(row.get("UNITA_POSOLOGICA")), row.get("UNITA_MIS_DDD"), getNumValue(row.get("GIORNI")) );
		}
		StringUtilities.outputWithTime("Finished loading source mappings");
	}

	//********************  ********************//
	private void loadSettings() {
		File f_old = new File("JCdmBuilder.ini");
		if(f_old.exists() && !f_old.isDirectory()) { 
			File f_new = new File("Genomedics.ini");
			if (!f_new.exists())
				try {
					f_old.renameTo(f_new);
				} catch (Exception e) {

				}
			else {
				f_old.delete();
			}
		}
		minimumFreeMemory = MINIMUM_FREE_MEMORY_DEFAULT;
		batchSize = BATCH_SIZE_DEFAULT;
		generateQaSamples = GENERATE_QASAMPLES_DEFAULT;
		qaSampleProbability = QA_SAMPLE_PROBABILITY_DEFAULT;

		File f = new File("Genomedics.ini");
		if(f.exists() && !f.isDirectory()) { 
			try {
				settings = new IniFile("Genomedics.ini");
				try {
					Long numParam = Long.valueOf(settings.get("memoryThreshold"));
					if (numParam != null)
						minimumFreeMemory = numParam;
				} catch (Exception e) {
					minimumFreeMemory = MINIMUM_FREE_MEMORY_DEFAULT;
				}

				try {
					Long numParam = Long.valueOf(settings.get("batchSize"));
					if (numParam != null)
						batchSize = numParam;
				} catch (Exception e) {
					batchSize = BATCH_SIZE_DEFAULT;
				}

				String hello = settings.get("hello");
				if (hello != null)
					StringUtilities.outputWithTime(hello);

				try {
					Long numParam = Long.valueOf(settings.get("generateQaSamples"));
					if (numParam != null) {
						if (numParam == 1)
							generateQaSamples = true;
						else
							generateQaSamples = false;
					}
				} catch (Exception e) {
					generateQaSamples = GENERATE_QASAMPLES_DEFAULT;
				}

				try {
					Double tmpDouble = null;
					String strParam = settings.get("qaSampleProbability");
					if ((strParam != null) && (!strParam.equals(""))) {
						tmpDouble = Double.valueOf(strParam);
						qaSampleProbability = tmpDouble;
					}
					else
						qaSampleProbability = QA_SAMPLE_PROBABILITY_DEFAULT;
				} catch (Exception e) {
					qaSampleProbability = QA_SAMPLE_PROBABILITY_DEFAULT;
				}

			} catch (Exception e) {
				StringUtilities.outputWithTime("No .ini file found");
			}
		}
	}

	//********************  ********************//
	private Double getNumValue(String val) {
		if ((val == null) || (val.equals("")))
			return null;
		else
			return Double.valueOf(val);
	}

	//******************** processPazientiSourceRecord ********************//
	private void processPazientiSourceRecord(Row row) {
		etlReport.registerIncomingData(PAZIENTI_TABLE, row);
		personId++;
		if (generateQaSamples)
			qcSampleConstructor.registerPersonData(PAZIENTI_TABLE, row, personId);

		if (addPersonAndRelatedRecords(row)) {
			processVisiteRecords(row);
			processPazpblRecords(row);
			processTerapTable(row);
			processPressRecords(row);
			processDescrizRecords(row);
			processAccertRecords(row);
			processRiabilRecords(row);
		}
	}

	//******************** PERSON ********************//

	private boolean addPersonAndRelatedRecords(Row row) {
		String codice = row.get("codice");

		// ** CREATE/STORE location_id		
		String provinciaRef = row.get("provincia");
		Long locationIdRef = provinciaToLocationId.get(provinciaRef);
		if (locationIdRef == null) { // Did not encounter this provincia code before
			locationIdRef = locationId;
			Row location = new Row();
			provinciaToLocationId.put(provinciaRef, locationId);
			location.add("location_id", locationId++);
			location.add("address_1", "");
			location.add("address_2", "");
			location.add("city", checkStringLength(row.get("regione"), 50, "location:city")); // NOTE!! Differs from original mapping!
			location.add("state", "");
			location.add("zip", "");
			location.add("county", checkStringLength(provinciaRef, 20, "location:county")); // NOTE!! Differs from original mapping!
			String srcVal = createSourceValue("location", "location_source_value", Long.toString(personId), 50, provinciaRef);
			location.add("location_source_value", srcVal);
			tableToRows.put("location", location);
		}

		// ** CREATE provider for id/userid, STORE id/userid vs. provider_id
		String providerRef = row.get(GPID_SOURCE_FIELD);
		Long refProviderId = gpidToProviderId.get(providerRef);
		if (refProviderId == null) { // Did not encounter this code before
			Row provider = new Row();
			refProviderId = providerId;
			gpidToProviderId.put(providerRef, providerId);
			codiceToProviderId.put(codice, providerId);
			provider.add("provider_id", providerId++);
			provider.add("provider_name", "");
			provider.add("npi", "");
			provider.add("dea", "");
			provider.add("specialty_concept_id", 38004446);  // General Practice
			provider.add("care_site_id", refProviderId);
			provider.add("year_of_birth", "");
			provider.add("gender_concept_id", "");
			provider.add("provider_source_value", checkStringLength(providerRef, 50, "provider_source_value"));
			provider.add("specialty_source_value", "");
			provider.add("specialty_source_concept_id", "");
			provider.add("gender_source_value", "");
			provider.add("gender_source_concept_id", "");
			tableToRows.put("provider", provider);

			// Care site is same as provider
			Row caresite = new Row();
			caresite.add("care_site_id", refProviderId);
			caresite.add("care_site_name", ""); 
			caresite.add("place_of_service_concept_id", "");
			caresite.add("location_id", locationIdRef);
			String srcVal = createSourceValue("care_site", "care_site_source_value", Long.toString(personId), 50, providerRef);
			caresite.add("care_site_source_value", srcVal);
			caresite.add("place_of_service_source_value", "");
			tableToRows.put("care_site", caresite);			
		}
		else {
			codiceToProviderId.put(codice, refProviderId);
		}

		Row person = new Row();
		person.add("person_id", personId);
		String genderSrc = row.get("sesso");
		person.add("gender_concept_id", genderSrc.equals("F") ? "8532" : genderSrc.equals("M") ? "8507" : "8551");
		String dateOfBirth = row.get("nascita");
		if (dateOfBirth.length() < 10) {
			person.add("year_of_birth", "");
			person.add("month_of_birth", "");
			person.add("day_of_birth", "");
		} else { 
			String yearStr = dateOfBirth.substring(0, 4);
			String monthStr = dateOfBirth.substring(5, 7);
			if (isInteger(yearStr)) {
				person.add("year_of_birth", yearStr);
				if (isInteger(monthStr))
					person.add("month_of_birth", monthStr);
				else
					person.add("month_of_birth", "");
			}
			else {
				person.add("year_of_birth", "");
				person.add("month_of_birth", "");
			}
			person.add("day_of_birth", ""); //** DAY NOT PROVIDED
		}
		person.add("time_of_birth", "");
		person.add("race_concept_id", 8552);	// Unknown (race)
		person.add("ethnicity_concept_id", 0); // No matching concept
		person.add("location_id", locationIdRef);
		person.add("provider_id", refProviderId);
		person.add("care_site_id", refProviderId);
		person.add("person_source_value", checkStringLength(codice, 50, "person_source_value"));
		person.add("gender_source_value", checkStringLength(genderSrc, 50, "gender_source_value"));
		person.add("race_source_value", "");
		person.add("race_source_concept_id", "");
		person.add("ethnicity_source_value", "");
		person.add("ethnicity_source_concept_id", "");
		tableToRows.put("person", person);

		// ** STORE codice vs. person_id
		codiceToPatientId.put(codice, personId);

		// ** CREATE observation period
		Row observationPeriod = new Row();
		observationPeriod.add("observation_period_id", observationPeriodId++);
		observationPeriod.add("person_id", personId);
		String startDate = row.get("data_in");
		String endDate = row.get("data_out");
		if (StringUtilities.isDate(startDate))
			observationPeriod.add("observation_period_start_date", startDate);
		else
			observationPeriod.add("observation_period_start_date", "");
		if (StringUtilities.isDate(endDate))
			observationPeriod.add("observation_period_end_date", endDate);
		else
			observationPeriod.add("observation_period_end_date", "");
		observationPeriod.add("period_type_concept_id", 44814724); // Period covering healthcare encounters
		tableToRows.put("observation_period", observationPeriod);

		// ** CREATE death record, if date of death provided
		String dateOfDeath = row.get("decesso");
		if (StringUtilities.isDate(dateOfDeath)) {
			Row death = new Row();
			death.add("person_id", personId);
			death.add("death_date", dateOfDeath);
			death.add("death_type_concept_id", 38003569); // EHR record patient status "Deceased"
			death.add("cause_concept_id", "");
			death.add("cause_source_value", "");
			death.add("cause_source_concept_id", "");
			tableToRows.put("death", death);
		}

		if (!useBatchInserts) {
			removeRowsWithNonNullableNulls();

			etlReport.registerOutgoingData(tableToRows);
			for (String table : tableToRows.keySet())
				targetConnection.insertIntoTable(tableToRows.get(table).iterator(), table, false, true);
			tableToRows.clear();
		}
		
		return true;
	}

	//********************************************************************************//
	private long processVisiteRecords(Row row) {
		String codice = row.get("codice");
		long numRecs = 0;

		for (Row visitRow : sourceConnection.query("SELECT * FROM "+ VISITE_TABLE + " WHERE codice = '" + codice + "'")) {
			String startDate = visitRow.get("data_open");
			if (StringUtilities.isDate(startDate)) {
				etlReport.registerIncomingData(VISITE_TABLE, visitRow);
				Row visitOccurrence = new Row();
				visitOccurrence.add("visit_occurrence_id", visitOccurrenceId++);
				visitOccurrence.add("person_id", personId);
				visitOccurrence.add("visit_concept_id", 9202); // Outpatient visit 
				visitOccurrence.add("visit_start_date", startDate);
				visitOccurrence.add("visit_start_time", checkStringLength(visitRow.get("ora"), 10, "visit_start_time[ora]"));
				visitOccurrence.add("visit_end_date", startDate);
				visitOccurrence.add("visit_end_time", "");
				visitOccurrence.add("visit_type_concept_id", 44818518); // Visit derived from EHR 
				Long refProviderId = codiceToProviderId.get(codice);
				visitOccurrence.add("provider_id", (refProviderId != null ? refProviderId.toString() : ""));
				visitOccurrence.add("care_site_id", (refProviderId != null ? refProviderId.toString() : ""));
				visitOccurrence.add("visit_source_value", "");
				visitOccurrence.add("visit_source_concept_id", "");
				tableToRows.put("visit_occurrence", visitOccurrence);
				numRecs++;
			}
		}

		if (!useBatchInserts) {
			removeRowsWithNonNullableNulls();

			etlReport.registerOutgoingData(tableToRows);
			targetConnection.insertIntoTable(tableToRows.get("visit_occurrence").iterator(), "visit_occurrence", false, true);
			tableToRows.clear();
		}

		return numRecs;
	}

	//********************************************************************************//
	private long processPazpblRecords(Row row) {
		String codice = row.get("codice");
		Long refProviderId = codiceToProviderId.get(codice);
		long numRecs = 0;
		String srcVal = "";

		for (Row pazpblRow : sourceConnection.query("SELECT * FROM "+ PAZPBL_TABLE + " WHERE codice = '" + codice + "'")) {
			etlReport.registerIncomingData(PAZPBL_TABLE, pazpblRow);
			String cpCode = pazpblRow.get("cp_code").trim();
			String cpCod2 = pazpblRow.get("cp_cod2").trim();
			String pbCode = pazpblRow.get("pb_code").trim();
			if ((cpCode != null) & (cpCode.length() > 0) & (!pbCode.equals("01")) & (!pbCode.equals("02"))) {
				CodeDomainData data = icd9ToConcept.getCodeData(cpCode);
				for (TargetConcept targetConcept : data.targetConcepts) {
					String targetDomain = targetConcept.domainId;
					String startDate = pazpblRow.get("data_open");
					if (!StringUtilities.isDate(startDate))
						startDate = "";
					switch (targetDomain) {
					case "Condition":
						Row conditionOccurrence = new Row();
						String concatKey = pazpblRow.get("codice") + pbCode;
						Long refConditionOccurrenceId = codicePbToConditionOccurrenceId.get(concatKey);
						if (refConditionOccurrenceId == null) { // Did not encounter this combination before
							codicePbToConditionOccurrenceId.put(concatKey, conditionOccurrenceId);
							conditionOccurrence.add("condition_occurrence_id", conditionOccurrenceId++);
							conditionOccurrence.add("person_id", personId);
							conditionOccurrence.add("condition_concept_id", targetConcept.conceptId);
							conditionOccurrence.add("condition_start_date", startDate);
							conditionOccurrence.add("condition_end_date", "");
							conditionOccurrence.add("condition_type_concept_id", 44786627);  // Primary condition
							conditionOccurrence.add("stop_reason", "");
							conditionOccurrence.add("provider_id", (refProviderId != null ? refProviderId.toString() : ""));
							conditionOccurrence.add("visit_occurrence_id", "");							
							if (cpCod2.length() == 0)
								srcVal = createSourceValue("condition_occurrence", "condition_source_value", Long.toString(personId), 50, cpCode);
							else
								srcVal = createSourceValue("condition_occurrence", "condition_source_value", Long.toString(personId), 50, cpCode + "/" + cpCod2);
							conditionOccurrence.add("condition_source_value", srcVal);
							conditionOccurrence.add("condition_source_concept_id", data.sourceConceptId);
							tableToRows.put("condition_occurrence", conditionOccurrence);
							numRecs++;
						}
						else {  // Already had this combination?
							etlReport.reportProblem("condition_occurrence", "Duplicate condition record; key: "+concatKey+", codice: "+codice, Long.toString(refConditionOccurrenceId));
							System.out.println("Duplicate condition record; key: "+concatKey+", codice: "+codice); 
						}
						break;

					case "Observation":
						Row observation = new Row();
						observation.add("observation_id", observationId++);
						observation.add("person_id", personId);
						observation.add("observation_concept_id", targetConcept.conceptId);
						observation.add("observation_date", startDate);
						observation.add("observation_time", "");
						observation.add("observation_type_concept_id", 38000280);// Observation recorded from EHR
						//TODO: confirm which value to record
						observation.add("value_as_number", ""); 
						observation.add("value_as_string", checkStringLength(pazpblRow.get("nome_pbl"), 60, "observatiopn:value_as_string[nome_pbl]")); //TODO: verify
						observation.add("value_as_concept_id", ""); 
						observation.add("qualifier_concept_id", ""); 
						observation.add("unit_concept_id", ""); 
						observation.add("provider_id", (refProviderId != null ? refProviderId.toString() : ""));
						observation.add("visit_occurrence_id", "");
						if (cpCod2.length() == 0)
							srcVal = createSourceValue("observation", "observation_source_value", Long.toString(personId), 50, cpCode);
						else
							srcVal = createSourceValue("observation", "observation_source_value", Long.toString(personId), 50, cpCode + "/" + cpCod2);
						observation.add("observation_source_value", srcVal);
						observation.add("observation_source_concept_id", data.sourceConceptId);
						observation.add("unit_source_value", "");
						observation.add("qualifier_source_value", "");
						tableToRows.put("observation", observation);
						numRecs++;
						break;

					case "Procedure":
						Row procedureOccurrence = new Row();
						procedureOccurrence.add("procedure_occurrence_id", procedureOccurrenceId++);
						procedureOccurrence.add("person_id", personId);
						procedureOccurrence.add("procedure_concept_id", targetConcept.conceptId);
						procedureOccurrence.add("procedure_date", startDate);
						procedureOccurrence.add("procedure_type_concept_id", 42865906);  // Condition Procedure
						procedureOccurrence.add("modifier_concept_id", "");
						procedureOccurrence.add("quantity", "");
						procedureOccurrence.add("provider_id", (refProviderId != null ? refProviderId.toString() : ""));
						procedureOccurrence.add("visit_occurrence_id", "");
						if (cpCod2.length() == 0)
							srcVal = createSourceValue("procedure_occurrence", "procedure_source_value", Long.toString(personId), 50, cpCode);
						else
							srcVal = createSourceValue("procedure_occurrence", "procedure_source_value", Long.toString(personId), 50, cpCode + "/" + cpCod2);
						procedureOccurrence.add("procedure_source_value", srcVal);
						procedureOccurrence.add("procedure_source_concept_id", data.sourceConceptId);
						procedureOccurrence.add("qualifier_source_value", "");
						tableToRows.put("procedure_occurrence", procedureOccurrence);
						numRecs++;
						break;

					case "Measurement":
						Row measurement = new Row();
						measurement.add("measurement_id", measurementId++);
						measurement.add("person_id", personId);
						measurement.add("measurement_concept_id", targetConcept.conceptId);
						measurement.add("measurement_date", startDate);
						measurement.add("measurement_time", "");
						measurement.add("measurement_type_concept_id", 44818701);  // From physical examination
						measurement.add("operator_concept_id", "");
						measurement.add("value_as_number", "");
						measurement.add("value_as_concept_id", "");
						measurement.add("unit_concept_id", "");
						measurement.add("range_low", "");
						measurement.add("range_high", "");
						measurement.add("provider_id", (refProviderId != null ? refProviderId.toString() : ""));
						measurement.add("visit_occurrence_id", "");
						if (cpCod2.length() == 0)
							srcVal = createSourceValue("measurement", "measurement_source_value", Long.toString(personId), 50, cpCode);
						else
							srcVal = createSourceValue("measurement", "measurement_source_value", Long.toString(personId), 50, cpCode + "/" + cpCod2);
						measurement.add("measurement_source_value", srcVal);
						measurement.add("measurement_source_concept_id", data.sourceConceptId);
						measurement.add("unit_source_value", "");
						srcVal = createSourceValue("measurement", "value_source_value", Long.toString(personId), 50, pazpblRow.get("nome_pbl"));
						measurement.add("value_source_value", srcVal);
						tableToRows.put("measurement", measurement);
						numRecs++;
						break;

					default:
						System.out.println("Other domain from "+PAZPBL_TABLE+":"+targetDomain+", cpCode: "+cpCode+", concept_id"+targetConcept.conceptId+", "+personId); 
						etlReport.reportProblem("condition_occurrence", "Other domain from "+PAZPBL_TABLE+":"+targetDomain+", cpCode: "+cpCode+", concept_id: "+targetConcept.conceptId, Long.toString(personId));
						break;
					}
				}
			}
		}
		if (!useBatchInserts) {
			removeRowsWithNonNullableNulls();

			etlReport.registerOutgoingData(tableToRows);
			targetConnection.insertIntoTable(tableToRows.get("condition_occurrence").iterator(), "condition_occurrence", false, true);
			targetConnection.insertIntoTable(tableToRows.get("procedure_occurrence").iterator(), "procedure_occurrence", false, true);
			targetConnection.insertIntoTable(tableToRows.get("observation").iterator(), "observation", false, true);
			targetConnection.insertIntoTable(tableToRows.get("measurement").iterator(), "measurement", false, true);
			tableToRows.clear();
		}
		return numRecs;
	}

	//********************************************************************************//
	private long processTerapTable(Row row) {
		String codice = row.get("codice");
		long numRecs = 0;

		for (Row terapRow : sourceConnection.query("SELECT * FROM "+ TERAP_TABLE + " WHERE codice = '" + codice + "'")) {
			etlReport.registerIncomingData(TERAP_TABLE, terapRow);
			String coAtc = terapRow.get("co_atc");
			String coCodifa = terapRow.get("co_codifa"); 
			if (coAtc.length() > 0 ) {
				CodeDomainData data = atcToConcept.getCodeData(coAtc);
				if (data.sourceConceptId != 0) {
					for (TargetConcept targetConcept : data.targetConcepts) {
						String targetDomain = targetConcept.domainId;
						switch (targetDomain) {
						case "Drug":
							Row drugExposure = new Row();
							drugExposure.add("drug_exposure_id", drugExposureId++);
							drugExposure.add("person_id", personId);
							drugExposure.add("drug_concept_id", targetConcept.conceptId);
							String startDate = terapRow.get("data_open");
							if (StringUtilities.isDate(startDate))
								drugExposure.add("drug_exposure_start_date", startDate);
							else
								drugExposure.add("drug_exposure_start_date", "");

							Double teNpezzi = null;
							String tmpS = terapRow.get("te_npezzi");
							if ((tmpS != null) && (!tmpS.equals("")))
								teNpezzi = Double.valueOf(tmpS);

							Double euroNum = null;
							String tmpS2 = terapRow.get("euro_num");
							if ((tmpS2 != null) && (!tmpS2.equals("")))
								euroNum = Double.valueOf(tmpS2);

							Double giorni = codifaToDurgDDD.getGiorni(coCodifa);							
							String deEndDate = null;
							int daysSupply = 0;
							if ((!startDate.equals("")) && (giorni != null) && (teNpezzi != null)) {
								daysSupply = (int) (teNpezzi * giorni);
								deEndDate = addDaysToDate(startDate, daysSupply);
								drugExposure.add("drug_exposure_end_date", deEndDate); 								
							}
							else
								drugExposure.add("drug_exposure_end_date", "");

							drugExposure.add("drug_type_concept_id", 38000177);  // Prescription written
							drugExposure.add("stop_reason", "");
							drugExposure.add("refills", "");
							
							Double quantPa = codifaToDurgDDD.getQuantPa(coCodifa);
							Double unitaPosologica = codifaToDurgDDD.getUnitaPosologica(coCodifa);
							if ((quantPa != null) && (unitaPosologica != null) && (unitaPosologica != 0)) {
								drugExposure.add("quantity", quantPa / unitaPosologica);
							}
							else
								drugExposure.add("quantity", ""); 
							
							if (deEndDate != null)
								drugExposure.add("days_supply", daysSupply);  
							else
								drugExposure.add("days_supply", "");  
							
							drugExposure.add("sig", terapRow.get("po_des"));
							
							String via = codifaToDurgDDD.getVia(coCodifa);
							int routeConceptId = viaToConcept.getConceptId(via);
							drugExposure.add("route_concept_id", routeConceptId);
							
							if ((quantPa != null) && (teNpezzi != null)) 
								drugExposure.add("effective_drug_dose", quantPa * teNpezzi);
							else
								drugExposure.add("effective_drug_dose", "");
							
							String umd = codifaToDurgDDD.getUnitaMisDDD(coCodifa);
							int unitConceptId = umdToConcept.getConceptId(umd);
							drugExposure.add("dose_unit_concept_id", unitConceptId);
							
							drugExposure.add("lot_number", "");
							Long refProviderId = codiceToProviderId.get(codice);
							drugExposure.add("provider_id", (refProviderId != null ? refProviderId.toString() : ""));
							
							drugExposure.add("visit_occurrence_id", "");
							
							String srcVal = createSourceValue("drug_exposure", "drug_source_value", Long.toString(personId), 50, terapRow.get("co_atc"));
							drugExposure.add("drug_source_value", srcVal); //TODO: change to co_codifa later
							drugExposure.add("drug_source_concept_id", data.sourceConceptId);
							srcVal = createSourceValue("drug_exposure", "route_source_value", Long.toString(personId), 50, codifaToDurgDDD.getVia(coCodifa));
							drugExposure.add("route_source_value", srcVal); 
							srcVal = createSourceValue("drug_exposure", "dose_unit_source_value", Long.toString(personId), 50, codifaToDurgDDD.getUnitaMisDDD(coCodifa));
							drugExposure.add("dose_unit_source_value", srcVal); 
							tableToRows.put("drug_exposure", drugExposure);
							numRecs++;
							
							// *********** drug_cost ******************
							if ((teNpezzi != null) && (euroNum != null)) {
								Row drugCost = new Row();
								drugCost.add("cost_id", drugCostId++);
								drugCost.add("cost_event_id", drugExposureId);
								drugCost.add("cost_domain_id", "Drug");
								drugCost.add("cost_type_concept_id", 0);
								drugCost.add("currency_concept_id", 44818568); // Euro
								drugCost.add("total_charge", "");
								drugCost.add("total_cost", "");
								long totalPaid = (long) (euroNum * teNpezzi);
								drugCost.add("total_paid", totalPaid);
								drugCost.add("paid_by_payer", "");
								drugCost.add("paid_by_patient", "");
								drugCost.add("paid_patient_copay", "");
								drugCost.add("paid_patient_coinsurance", "");
								drugCost.add("paid_patient_deductible", "");
								drugCost.add("paid_by_primary", "");
								drugCost.add("paid_ingredient_cost", "");
								drugCost.add("paid_dispensing_fee", "");
								drugCost.add("payer_plan_period_id", "");
								drugCost.add("amount_allowed", "");
								drugCost.add("revenue_code_concept_id", "");
								drugCost.add("revenue_code_source_value", "");
								tableToRows.put("cost", drugCost);
							}
							break;
						default:
							System.out.println("Other domain from DRUGS:"+targetDomain+", "+terapRow.get("co_atc")+", "+targetConcept.conceptId+", "+personId); 
							etlReport.reportProblem("drug_exposure", "Other domain from DRUGS:"+targetDomain+", "+terapRow.get("co_atc")+", "+targetConcept.conceptId, Long.toString(personId));
							break;
						}
					}
				} else {
					//etlReport.reportProblem("drug_exposure", "No Standard drug_concept_id available for ATC code "+coAtc, Long.toString(personId));
					//System.out.println("ATC lookup failed: "+coAtc+", codice: "+codice); 
				}
			}
		}
		if (!useBatchInserts) {
			removeRowsWithNonNullableNulls();

			etlReport.registerOutgoingData(tableToRows);
			targetConnection.insertIntoTable(tableToRows.get("drug_exposure").iterator(), "drug_exposure", false, true);
			tableToRows.clear();
		}

		return numRecs;
	}

	//********************************************************************************//
	private long processPressRecords(Row row) {
		String codice = row.get("codice");
		Long refProviderId = codiceToProviderId.get(codice);
		long numRecs = 0;

		for (Row pressRow : sourceConnection.query("SELECT * FROM "+ PRESS_TABLE + " WHERE codice = '" + codice + "'")) {
			etlReport.registerIncomingData(PRESS_TABLE, pressRow);
			String pMin = pressRow.get("p_min");
			String pMax = pressRow.get("p_max");
			String frequenza = pressRow.get("frequenza");
			String startDate = pressRow.get("data_open");

			if ((pMin.length() > 0) && (StringUtilities.isNumber(pMin))) {
				Row measurement = new Row();
				measurement.add("measurement_id", measurementId++);
				measurement.add("person_id", personId);
				measurement.add("measurement_concept_id", 3012888); // BP diastolic
				if (StringUtilities.isDate(startDate))
					measurement.add("measurement_date", startDate);
				else
					measurement.add("measurement_date", "");
				measurement.add("measurement_time", "");
				measurement.add("measurement_type_concept_id", 44818701);  // From physical examination
				measurement.add("operator_concept_id", "");
				measurement.add("value_as_number", Double.parseDouble(pMin));
				measurement.add("value_as_concept_id", "");
				measurement.add("unit_concept_id", 8876); // millimeter mercury column (mm[Hg])
				measurement.add("range_low", "");
				measurement.add("range_high", "");
				measurement.add("provider_id", (refProviderId != null ? refProviderId.toString() : ""));
				measurement.add("visit_occurrence_id", "");
				measurement.add("measurement_source_value", "");
				measurement.add("measurement_source_concept_id", "");
				measurement.add("unit_source_value", "");
				String srcVal = createSourceValue("measurement", "value_source_value", Long.toString(personId), 50, pMin);
				measurement.add("value_source_value", srcVal);
				tableToRows.put("measurement", measurement);
				numRecs++;
			}
			if ((pMax.length() > 0) && (StringUtilities.isNumber(pMax))) {  
				Row measurement = new Row();
				measurement.add("measurement_id", measurementId++);
				measurement.add("person_id", personId);
				measurement.add("measurement_concept_id", 3004249); // BP systolic
				if (StringUtilities.isDate(startDate))
					measurement.add("measurement_date", startDate);
				else
					measurement.add("measurement_date", "");
				measurement.add("measurement_time", "");
				measurement.add("measurement_type_concept_id", 44818701);  // From physical examination
				measurement.add("operator_concept_id", "");
				measurement.add("value_as_number", Double.parseDouble(pMax));
				measurement.add("value_as_concept_id", "");
				measurement.add("unit_concept_id", 8876); // millimeter mercury column (mm[Hg])
				measurement.add("range_low", "");
				measurement.add("range_high", "");
				measurement.add("provider_id", (refProviderId != null ? refProviderId.toString() : ""));
				measurement.add("visit_occurrence_id", "");
				measurement.add("measurement_source_value", "");
				measurement.add("measurement_source_concept_id", "");
				measurement.add("unit_source_value", "");
				String srcVal = createSourceValue("measurement", "value_source_value", Long.toString(personId), 50, pMax);
				measurement.add("value_source_value", srcVal);
				tableToRows.put("measurement", measurement);
				numRecs++;
			}
			if ((frequenza.length() > 0) && (StringUtilities.isNumber(frequenza))) { 
				Row measurement = new Row();
				measurement.add("measurement_id", measurementId++);
				measurement.add("person_id", personId);
				measurement.add("measurement_concept_id", 3027018); // Heart rate
				if (StringUtilities.isDate(startDate))
					measurement.add("measurement_date", startDate);
				else
					measurement.add("measurement_date", "");
				measurement.add("measurement_time", "");
				measurement.add("measurement_type_concept_id", 44818701);  // From physical examination
				measurement.add("operator_concept_id", "");
				measurement.add("value_as_number", Double.parseDouble(frequenza));
				measurement.add("value_as_concept_id", "");
				measurement.add("unit_concept_id", 8581); // heartbeat
				measurement.add("range_low", "");
				measurement.add("range_high", "");
				measurement.add("provider_id", (refProviderId != null ? refProviderId.toString() : ""));
				measurement.add("visit_occurrence_id", "");
				measurement.add("measurement_source_value", "");
				measurement.add("measurement_source_concept_id", "");
				measurement.add("unit_source_value", "");
				String srcVal = createSourceValue("measurement", "value_source_value", Long.toString(personId), 50, frequenza);
				measurement.add("value_source_value", srcVal);
				tableToRows.put("measurement", measurement);
				numRecs++;
			}
		}

		if (!useBatchInserts) {
			removeRowsWithNonNullableNulls();

			etlReport.registerOutgoingData(tableToRows);
			targetConnection.insertIntoTable(tableToRows.get("measurement").iterator(), "measurement", false, true);
			tableToRows.clear();
		}

		return numRecs;
	}

	//********************************************************************************//
	private long processAccertRecords(Row row) {
		String codice = row.get("codice");
		long numRecs = 0;
		Long refProviderId = codiceToProviderId.get(codice);
		String srcVal = "";
		
		for (Row accertRow : sourceConnection.query("SELECT * FROM "+ ACCERT_TABLE + " WHERE codice = '" + codice + "'")) {
			etlReport.registerIncomingData(ACCERT_TABLE, accertRow);
			String pbCode = accertRow.get("pb_code").trim();
			if ( (!pbCode.equals("01")) & (!pbCode.equals("02")) ) {
				String hsCode = accertRow.get("hs_code").trim();
				String startDate = accertRow.get("data_open");
				CodeDomainData data = hsCodeToConcept.getCodeData(hsCode);
				for (TargetConcept targetConcept : data.targetConcepts) {
					Double acValNum = null;
					String tmpS1 = accertRow.get("ac_val_num");
					if ((tmpS1 != null) && (!tmpS1.equals("")))
						acValNum = Double.valueOf(tmpS1);
					String targetDomain = targetConcept.domainId;
					switch (targetDomain) {

					case "Observation":
						Row observation = new Row();
						observation.add("observation_id", observationId++);
						observation.add("person_id", personId);
						observation.add("observation_concept_id", targetConcept.conceptId);
						if (StringUtilities.isDate(startDate))
							observation.add("observation_date", startDate);
						else
							observation.add("observation_date", "");
						observation.add("observation_time", "");
						observation.add("observation_type_concept_id", 38000280);// Observation recorded from EHR
						if (acValNum != null)
							observation.add("value_as_number", acValNum); 
						else
							observation.add("value_as_number", ""); 
						observation.add("value_as_string", ""); 
						observation.add("value_as_concept_id", ""); 
						observation.add("qualifier_concept_id", ""); 
						observation.add("unit_concept_id", ""); //TODO: depends on observation_concept_id
						observation.add("provider_id", (refProviderId != null ? refProviderId.toString() : ""));
						observation.add("visit_occurrence_id", "");
						srcVal = createSourceValue("observation", "observation_source_value", Long.toString(personId), 50, hsCode);
						observation.add("observation_source_value", srcVal);
						observation.add("observation_source_concept_id", data.sourceConceptId);
						observation.add("unit_source_value", "");
						observation.add("qualifier_source_value", "");
						tableToRows.put("observation", observation);
						numRecs++;
						break;

					case "Procedure":
						Row procedureOccurrence = new Row();
						procedureOccurrence.add("procedure_occurrence_id", procedureOccurrenceId++);
						procedureOccurrence.add("person_id", personId);
						procedureOccurrence.add("procedure_concept_id", targetConcept.conceptId);
						if (StringUtilities.isDate(startDate))
							procedureOccurrence.add("procedure_date", startDate);
						else
							procedureOccurrence.add("procedure_date", "");
						procedureOccurrence.add("procedure_type_concept_id", 44818701);  // From physical examination
						procedureOccurrence.add("modifier_concept_id", "");
						procedureOccurrence.add("quantity", "");
						procedureOccurrence.add("provider_id", (refProviderId != null ? refProviderId.toString() : ""));
						procedureOccurrence.add("visit_occurrence_id", "");
						srcVal = createSourceValue("procedure_occurrence", "procedure_source_value", Long.toString(personId), 50, hsCode);
						procedureOccurrence.add("procedure_source_value", srcVal);
						procedureOccurrence.add("procedure_source_concept_id", data.sourceConceptId);
						procedureOccurrence.add("qualifier_source_value", "");
						tableToRows.put("procedure_occurrence", procedureOccurrence);
						numRecs++;

						Double euroNum = null;
						String tmpS = accertRow.get("euro_num");
						if ((tmpS != null) && (!tmpS.equals("")))
							euroNum = Double.valueOf(tmpS);
						if (euroNum != null) {
							Row procedureCost = new Row();
							procedureCost.add("cost_id", procedureCostId++);
							procedureCost.add("cost_event_id", procedureOccurrenceId);
							procedureCost.add("cost_domain_id", "Procedure");
							procedureCost.add("cost_type_concept_id", 0);
							procedureCost.add("currency_concept_id", 44818568); // Euro
							procedureCost.add("total_charge", "");
							procedureCost.add("total_cost", "");
							procedureCost.add("total_paid", euroNum);
							procedureCost.add("paid_by_payer", "");
							procedureCost.add("paid_by_patient", "");
							procedureCost.add("paid_patient_copay", "");
							procedureCost.add("paid_patient_coinsurance", "");
							procedureCost.add("paid_patient_deductible", "");
							procedureCost.add("paid_by_primary", "");
							procedureCost.add("paid_ingredient_cost", "");
							procedureCost.add("paid_dispensing_fee", "");
							procedureCost.add("payer_plan_period_id", "");
							procedureCost.add("amount_allowed", "");
							procedureCost.add("revenue_code_concept_id", "");
							procedureCost.add("revenue_code_source_value", "");
							tableToRows.put("cost", procedureCost);
						}
						break;

					case "Measurement":
					case "Meas Value":
						Row measurement = new Row();
						measurement.add("measurement_id", measurementId++);
						measurement.add("person_id", personId);
						measurement.add("measurement_concept_id", targetConcept.conceptId);
						if (StringUtilities.isDate(startDate))
							measurement.add("measurement_date", startDate);
						else
							measurement.add("measurement_date", "");
						measurement.add("measurement_time", "");
						measurement.add("measurement_type_concept_id", 44818701);  // From physical examination
						Long opConceptId = extractOperatorConceptId(accertRow.get("ac_val"));
						if (opConceptId != null)
							measurement.add("operator_concept_id", opConceptId);
						else
							measurement.add("operator_concept_id", "");
						if (acValNum != null)
							measurement.add("value_as_number", acValNum);
						else
							measurement.add("value_as_number", "");
						measurement.add("value_as_concept_id", "");
						measurement.add("unit_concept_id", "");  //TODO: depends on measurement_concept_id
						measurement.add("range_low", "");
						measurement.add("range_high", "");
						measurement.add("provider_id", (refProviderId != null ? refProviderId.toString() : ""));
						measurement.add("visit_occurrence_id", "");
						srcVal = createSourceValue("measurement", "measurement_source_value", Long.toString(personId), 50, hsCode);
						measurement.add("measurement_source_value", srcVal);
						measurement.add("measurement_source_concept_id", 0);  //data.sourceConceptId);
						measurement.add("unit_source_value", "");
						srcVal = createSourceValue("measurement", "value_source_value", Long.toString(personId), 50, accertRow.get("ac_val"));
						measurement.add("value_source_value", srcVal);
						tableToRows.put("measurement", measurement);
						numRecs++;
						break;

					default:
						System.out.println("Other domain from "+ACCERT_TABLE+":"+targetDomain+", pbCode: "+pbCode+", concept_id"+targetConcept.conceptId+", "+personId); 
						etlReport.reportProblem("condition_occurrence", "Other domain from "+ACCERT_TABLE+":"+targetDomain+", pbCode: "+pbCode+", concept_id: "+targetConcept.conceptId, Long.toString(personId));
						break;
					}
				}
			}
			if (!useBatchInserts) {
				removeRowsWithNonNullableNulls();

				etlReport.registerOutgoingData(tableToRows);
				targetConnection.insertIntoTable(tableToRows.get("condition_occurrence").iterator(), "condition_occurrence", false, true);
				targetConnection.insertIntoTable(tableToRows.get("procedure_occurrence").iterator(), "procedure_occurrence", false, true);
				targetConnection.insertIntoTable(tableToRows.get("observation").iterator(), "observation", false, true);
				targetConnection.insertIntoTable(tableToRows.get("measurement").iterator(), "measurement", false, true);
				tableToRows.clear();
			}
		}

		return numRecs;
	}

	//********************************************************************************//
	private long processDescrizRecords(Row row) {
		String codice = row.get("codice");
		long numRecs = 0;

		for (Row descrizRow : sourceConnection.query("SELECT * FROM "+ DESCRIZ_TABLE + " WHERE codice = '" + codice + "'")) {
			etlReport.registerIncomingData(DESCRIZ_TABLE, descrizRow);
			String noteText = descrizRow.get("de_descr");
			String noteDate = descrizRow.get("data_open");
			if (noteText.length() > 0) {
				Row noteOccurrence = new Row();
				noteOccurrence.add("note_id", noteId++);
				noteOccurrence.add("person_id", personId);
				if (StringUtilities.isDate(noteDate))
					noteOccurrence.add("note_date", noteDate);
				else
					noteOccurrence.add("note_date", "");
				noteOccurrence.add("note_time", "");
				noteOccurrence.add("note_type_concept_id", 44814645); // Note
				noteOccurrence.add("note_text", noteText);		
				Long refProviderId = codiceToProviderId.get(codice);
				noteOccurrence.add("provider_id", (refProviderId != null ? refProviderId.toString() : ""));
				noteOccurrence.add("note_source_value", "");
				noteOccurrence.add("visit_occurrence_id", "");
				tableToRows.put("note", noteOccurrence);
				numRecs++;
			}
		}
		if (!useBatchInserts) {
			removeRowsWithNonNullableNulls();

			etlReport.registerOutgoingData(tableToRows);
			targetConnection.insertIntoTable(tableToRows.get("note").iterator(), "note", false, true);
			tableToRows.clear();
		}

		return numRecs;
	}

	//********************************************************************************//
	private long processRiabilRecords(Row row) {
		String codice = row.get("codice");
		Long refProviderId = codiceToProviderId.get(codice);
		long numRecs = 0;

		for (Row riabilRow : sourceConnection.query("SELECT * FROM "+ RIABIL_TABLE + " WHERE codice = '" + codice + "'")) {
			etlReport.registerIncomingData(RIABIL_TABLE, riabilRow);
			String hsCode = riabilRow.get("hs_code").trim();
			String startDate = riabilRow.get("data_open");
			CodeDomainData data = hsCodeToConcept.getCodeData(hsCode);
			for (TargetConcept targetConcept : data.targetConcepts) {
				String targetDomain = targetConcept.domainId;
				Integer riQta = null;
				String tmpS = riabilRow.get("ri_qta");
				if ((tmpS != null) && (!tmpS.equals(""))) {
					Double tmpD = Double.valueOf(tmpS);
					if (tmpD != null)
						riQta = tmpD.intValue();
				}
				Double euroNum = null;
				String tmpS1 = riabilRow.get("euro_num");
				if ((tmpS1 != null) && (!tmpS1.equals("")))
					euroNum = Double.valueOf(tmpS1);

				switch (targetDomain) {
				case "Procedure":
					Row procedureOccurrence = new Row();
					procedureOccurrence.add("procedure_occurrence_id", procedureOccurrenceId++);
					procedureOccurrence.add("person_id", personId);
					procedureOccurrence.add("procedure_concept_id", targetConcept.conceptId);
					if (StringUtilities.isDate(startDate))
						procedureOccurrence.add("procedure_date", startDate);
					else
						procedureOccurrence.add("procedure_date", "");
					procedureOccurrence.add("procedure_type_concept_id", 42865906);  //TODO: Check! Condition Procedure
					procedureOccurrence.add("modifier_concept_id", "");
					if (riQta!=null)
						procedureOccurrence.add("quantity", riQta);
					else
						procedureOccurrence.add("quantity", "");
					procedureOccurrence.add("provider_id", (refProviderId != null ? refProviderId.toString() : ""));
					procedureOccurrence.add("visit_occurrence_id", "");
					String srcVal = createSourceValue("procedure_occurrence", "procedure_source_value", Long.toString(personId), 50, hsCode);
					procedureOccurrence.add("procedure_source_value", srcVal);
					procedureOccurrence.add("procedure_source_concept_id", 0);
					procedureOccurrence.add("qualifier_source_value", "");
					tableToRows.put("procedure_occurrence", procedureOccurrence);
					numRecs++;

					if ((riQta != null) && (euroNum != null)) {
						Row procedureCost = new Row();
						procedureCost.add("cost_id", procedureCostId++);
						procedureCost.add("cost_event_id", procedureOccurrenceId);
						procedureCost.add("cost_domain_id", "Procedure");
						procedureCost.add("cost_type_concept_id", 0);
						procedureCost.add("currency_concept_id", 44818568); // Euro
						procedureCost.add("total_charge", "");
						procedureCost.add("total_cost", "");
						procedureCost.add("total_paid", (riQta * euroNum));
						procedureCost.add("paid_by_payer", "");
						procedureCost.add("paid_by_patient", "");
						procedureCost.add("paid_patient_copay", "");
						procedureCost.add("paid_patient_coinsurance", "");
						procedureCost.add("paid_patient_deductible", "");
						procedureCost.add("paid_by_primary", "");
						procedureCost.add("paid_ingredient_cost", "");
						procedureCost.add("paid_dispensing_fee", "");
						procedureCost.add("payer_plan_period_id", "");
						procedureCost.add("amount_allowed", "");
						procedureCost.add("revenue_code_concept_id", "");
						procedureCost.add("revenue_code_source_value", "");
						tableToRows.put("cost", procedureCost);
					}
					break;

				case "Device":
					Row deviceExposure = new Row();
					deviceExposure.add("device_exposure_id", deviceExposureId++);
					deviceExposure.add("person_id", personId);
					deviceExposure.add("device_concept_id", targetConcept.conceptId);
					if (StringUtilities.isDate(startDate))
						deviceExposure.add("device_exposure_start_date", startDate);
					else
						deviceExposure.add("device_exposure_start_date", "");
					deviceExposure.add("device_exposure_end_date", "");
					deviceExposure.add("device_type_concept_id", 44818707);  // EHR Detail 
					deviceExposure.add("unique_device_id", "");
					if (riQta!=null)
						deviceExposure.add("quantity", riQta);
					else
						deviceExposure.add("quantity", "");
					deviceExposure.add("provider_id", (refProviderId != null ? refProviderId.toString() : ""));
					deviceExposure.add("visit_occurrence_id", "");
					srcVal = createSourceValue("device_exposure", "device_source_value", Long.toString(personId), 100, hsCode);
					deviceExposure.add("device_source_value", srcVal);
					deviceExposure.add("device_source_concept_id", data.sourceConceptId);
					tableToRows.put("device_exposure", deviceExposure);
					numRecs++;

					if ((riQta != null) && (euroNum != null)) {
						Row deviceCost = new Row();
						deviceCost.add("cost_id", deviceCostId++);
						deviceCost.add("cost_event_id", deviceExposureId);
						deviceCost.add("cost_domain_id", "Device");
						deviceCost.add("cost_type_concept_id", 0);
						deviceCost.add("currency_concept_id", 44818568); // Euro
						deviceCost.add("total_charge", "");
						deviceCost.add("total_cost", "");
						deviceCost.add("total_paid", (riQta * euroNum));
						deviceCost.add("paid_by_payer", "");
						deviceCost.add("paid_by_patient", "");
						deviceCost.add("paid_patient_copay", "");
						deviceCost.add("paid_patient_coinsurance", "");
						deviceCost.add("paid_patient_deductible", "");
						deviceCost.add("paid_by_primary", "");
						deviceCost.add("paid_ingredient_cost", "");
						deviceCost.add("paid_dispensing_fee", "");
						deviceCost.add("payer_plan_period_id", "");
						deviceCost.add("amount_allowed", "");
						deviceCost.add("revenue_code_concept_id", "");
						deviceCost.add("revenue_code_source_value", "");
						tableToRows.put("cost", deviceCost);
					}
					break;

				default:
					System.out.println("Other domain from "+RIABIL_TABLE+":"+targetDomain+", riCode: "+hsCode+", concept_id: "+targetConcept.conceptId+", "+personId); 
					etlReport.reportProblem("condition_occurrence", "Other domain from "+PAZPBL_TABLE+":"+targetDomain+", riCode: "+hsCode+", concept_id: "+targetConcept.conceptId, Long.toString(personId));
					break;
				}
			}
		}

		if (!useBatchInserts) {
			removeRowsWithNonNullableNulls();

			etlReport.registerOutgoingData(tableToRows);
			targetConnection.insertIntoTable(tableToRows.get("procedure_occurrence").iterator(), "procedure_occurrence", false, true);
			tableToRows.clear();
		}

		return numRecs;
	}
	//********************  ********************//
	private void removeRowsOutsideOfObservationTime() {
		List<Row> observationPeriods = tableToRows.get("observation_period");
		long[] startDates = new long[observationPeriods.size()];
		long[] endDates = new long[observationPeriods.size()];
		int i = 0;
		Iterator<Row> observationPeriodIterator = observationPeriods.iterator();
		while (observationPeriodIterator.hasNext()) {
			Row observationPeriod = observationPeriodIterator.next();
			try {
				startDates[i] = StringUtilities.databaseTimeStringToDays(observationPeriods.get(i).get("observation_period_start_date"));
			} catch (Exception e) {
				etlReport.reportProblem("observation_period", "Illegal observation_period_start_date. Removing person", observationPeriod.get("person_id"));
				observationPeriodIterator.remove();
				startDates[i] = StringUtilities.MISSING_DATE;
				endDates[i] = StringUtilities.MISSING_DATE;
				continue;
			}
			try {
				endDates[i] = StringUtilities.databaseTimeStringToDays(observationPeriods.get(i).get("observation_period_end_date"));
			} catch (Exception e) {
				etlReport.reportProblem("observation_period", "Illegal observation_period_end_date. Removing person", observationPeriod.get("person_id"));
				observationPeriodIterator.remove();
				startDates[i] = StringUtilities.MISSING_DATE;
				endDates[i] = StringUtilities.MISSING_DATE;
				continue;
			}
			i++;
		}

		for (i = 0; i < tablesInsideOP.length; i++) {
			Iterator<Row> iterator = tableToRows.get(tablesInsideOP[i]).iterator();
			while (iterator.hasNext()) {
				Row row = iterator.next();
				boolean insideOP = false;
				try {
					long rowDate = StringUtilities.databaseTimeStringToDays(row.get(fieldInTables[i]));
					for (int j = 0; j < startDates.length; j++) {
						if (rowDate >= startDates[j] && rowDate <= endDates[j]) {
							insideOP = true;
							break;
						}
					}
					if (!insideOP) {
						iterator.remove();
						etlReport.reportProblem(tablesInsideOP[i], "Data outside observation period. Removed data", row.get("person_id"));
					}
				} catch (Exception e) {
					etlReport.reportProblem(tablesInsideOP[i], "Illegal " + fieldInTables[i] + ". Cannot add row", row.get("person_id"));
					iterator.remove();
					continue;
				}
			}
		}
	}

	//********************  ********************//
	private void removeRowsWithNonNullableNulls() {
		for (String table : tableToRows.keySet()) {
			Iterator<Row> iterator = tableToRows.get(table).iterator();
			while (iterator.hasNext()) {
				Row row = iterator.next();
				String nonAllowedNullField = cdmv5NullableChecker.findNonAllowedNull(table, row);
				if (nonAllowedNullField != null) {
					if (row.getFieldNames().contains("person_id"))
						etlReport.reportProblem(table, "Column " + nonAllowedNullField + " is null, could not create row", row.get("person_id"));
					else
						etlReport.reportProblem(table, "Column " + nonAllowedNullField + " is null, could not create row", "");
					iterator.remove();
				}
			}
		}
	}

	//********************  ********************//
	private void createCostTable(DbSettings dbSettings) {
		String sqlFile = "sql/CreateCostTable - SQL Server.sql";
		if (dbSettings.dbType == DbType.ORACLE) {
			sqlFile = "sql/CreateCostTable - SQL Server.sql";
		} else if (dbSettings.dbType == DbType.MSSQL) {
			sqlFile = "sql/CreateCostTable - SQL Server.sql";
		} else if (dbSettings.dbType == DbType.POSTGRESQL) {
			sqlFile = "sql/CreateCostTable - PostgreSQL.sql";
		}
		targetConnection.executeResource(sqlFile);
	}

	//********************  ********************//
	private Long extractOperatorConceptId(String val) {
		Long conceptId = null;
		if (val.length() > 1) {
			if ( val.startsWith("<") | val.startsWith(">")) {
				String operatorPart = "";
				if (Character.toString(val.charAt(1)) == "=") // <= or >=
					operatorPart = val.substring(0, 2);
				else
					operatorPart = val.substring(0, 1);
				if (StringUtilities.isNumber(val.substring(operatorPart.length()))) {
					switch (operatorPart) {
					case "<":
						conceptId = (long) 4171756;
						break;
					case ">":
						conceptId = (long) 4172704;
						break;
					case "=":
						conceptId = (long) 4172703;
						break;
					case "<=":
						conceptId = (long) 4171754;
						break;
					case ">=":
						conceptId = (long) 4171755;
						break;
					default:
						break;
					}
				}
			}
		}
		return conceptId;
	}

	//********************  ********************//
	private boolean memoryGettingLow() {
		long availableMemory = calculatedFreeMemory();
		return (availableMemory < minimumFreeMemory);
	}

	//********************  ********************//
	private String createSourceValue(String destTable, String destField, String personId, int maxLength, String sourceValue) {
		String retString = "";
		int srcLength = sourceValue.length();
		if (srcLength > 0) {
			if (srcLength > maxLength) {
				retString = sourceValue.substring(0, maxLength - 4) + "...";
				etlReport.reportProblem(destTable, destField + " value truncated.", personId);
				String message = "'" + destTable + "." + destField + "' truncated - source length: " + Long.toString(srcLength) + ", target size: " + Long.toString(maxLength) + ((char) 13) + ((char) 10);
				message += "- Source string: " + sourceValue + ((char) 13) + ((char) 10);
				message += "- Stored in target field: " + retString + ((char) 13) + ((char) 10);
				writeToLog(message + ((char) 13) + ((char) 10));
			}
			else
				retString = sourceValue;
		}
		return retString;
	}

	//********************  ********************//
	private String checkStringLength(String sourceValue, int maxLength, String refStr) {
		String retString = "";
		int srcLength = sourceValue.length();
		if (srcLength > 0) {
			if (srcLength > maxLength) {
				retString = sourceValue.substring(0, maxLength - 4) + "...";
				String message = "'" + refStr + "': string truncated - source length: " + Long.toString(srcLength) + ", target size: " + Long.toString(maxLength) + ((char) 13) + ((char) 10);
				message += "- Source string: " + sourceValue + ((char) 13) + ((char) 10);
				message += "- Stored in target field: " + retString + ((char) 13) + ((char) 10);
				writeToLog(message + ((char) 13) + ((char) 10));
			}
			else
				retString = sourceValue;
		}
		return retString;
	}

	//********************  ********************//
	public static boolean isInteger(String s) {
	    if(s.isEmpty()) return false;
	    for(int i = 0; i < s.length(); i++) {
	        if(i == 0 && s.charAt(i) == '-') {
	            if(s.length() == 1) return false;
	            else continue;
	        }
	        if(Character.digit(s.charAt(i),10) < 0) return false;
	    }
	    return true;
	}
	
}
