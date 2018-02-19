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
package org.ohdsi.jCdmBuilder.etls.jacse;

import java.io.File;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.ohdsi.databases.DbType;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.jCdmBuilder.DbSettings;
import org.ohdsi.jCdmBuilder.EtlReport;
import org.ohdsi.jCdmBuilder.ObjectExchange;
import org.ohdsi.jCdmBuilder.cdm.CdmV5NullableChecker;
import org.ohdsi.jCdmBuilder.utilities.CSVFileChecker;
import org.ohdsi.jCdmBuilder.utilities.CodeToConceptMap;
import org.ohdsi.jCdmBuilder.utilities.CodeToDomainConceptMap;
import org.ohdsi.jCdmBuilder.etls.jacse.SourceToTargetTrack;
//import org.ohdsi.jCdmBuilder.utilities.ETLUtils;
import org.ohdsi.jCdmBuilder.utilities.QCSampleConstructor;
import org.ohdsi.jCdmBuilder.utilities.CodeToDomainConceptMap.CodeDomainData;
import org.ohdsi.jCdmBuilder.utilities.CodeToDomainConceptMap.TargetConcept;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.collections.OneToManyList;
import org.ohdsi.utilities.collections.RowUtilities;
import org.ohdsi.utilities.files.FileSorter;
import org.ohdsi.utilities.files.IniFile;
import org.ohdsi.utilities.files.MultiRowIterator;
import org.ohdsi.utilities.files.MultiRowIterator.MultiRowSet;
import org.ohdsi.utilities.files.ReadCSVFileWithHeader;
import org.ohdsi.utilities.files.ReadTextFile;
import org.ohdsi.utilities.files.Row;

public class JACSEETLtoV5 {

	public static String				EXTRACTION_DATE_DEFAULT			= "2017-12-31";
	public static String				MIN_OBSERVATION_DATE_DEFAULT	= "2003-01-01";
	public static String				DATE_PLACEHOLDER_DEFAULT		= "9999-12-31";
	public static String				REGISTRATION_DATE_DEFAULT		= "1967-01-01";
	public static int					MAX_ROWS_PER_PERSON_DEFAULT		= 100000;
	public static long					BATCH_SIZE_DEFAULT				= 1000;
	public static boolean				GENERATE_QASAMPLES_DEFAULT		= false;
	public static double				QA_SAMPLE_PROBABILITY_DEFAULT	= 0.001;
	public static String[]				TABLES							= new String[] { "PATIENT", "CANCER", "DEATH", "DRUG" };
	public static String[]				ICD_FIELDS						= new String[] { "icdo3", "icdo10", "icd9", "icd7" };
	//	public static int					BATCH_SIZE						= 1000;
	public static String[]				tablesInsideOP					= new String[] { "condition_occurrence", "procedure_occurrence", "drug_exposure", "death",
			"visit_occurrence", "observation", "measurement"					};
	public static String[]				fieldInTables					= new String[] { "condition_start_date", "procedure_date", "drug_exposure_start_date",
			"death_date", "visit_start_date", "observation_date", "measurement_date" };
	public static boolean				USE_VERBOSE_SOURCE_VALUES		= false;

	private int							personCount;
	private String						folder;
	private RichConnection				connection;
	private long						personId;
	private long						observationPeriodId;
	private long						drugExposureId;
	private long						conditionOccurrenceId;
	private long						visitOccurrenceId;
	private long						procedureOccurrenceId;
	private long						providerId;
	private long						observationId;
	private long						measurementId;
	private long						extractionDate;
	private long						registrationDate;
	private long						minObservationDate;

	private OneToManyList<String, Row>	tableToRows;
	private CodeToDomainConceptMap		atcToConcept;
	private Map<String, String>			procToType;
	private CodeToDomainConceptMap		icd9ToConcept;
	private CodeToDomainConceptMap		icd9ProcToConcept;
	private CodeToDomainConceptMap		icd10ToConcept;
	private Map<String, Long>			sourceToProviderId				= new HashMap<String, Long>();
	private Map<String, Long>			sourceToVisitId					= new HashMap<String, Long>();
	private Map<String, Long>			sourceToPersonId				= new HashMap<String, Long>();
	private QCSampleConstructor			qcSampleConstructor;
	private EtlReport					etlReport;
	private CdmV5NullableChecker		cdmv4NullableChecker			= new CdmV5NullableChecker();
	private SourceToTargetTrack			s2t;
	private Map<String, String>			patientKeyToFieldName			= new HashMap<String, String>();
	private Map<String, String>			cancerKeyToFieldName			= new HashMap<String, String>();
	private Map<String, String>			deathKeyToFieldName				= new HashMap<String, String>();
	private Map<String, String>			drugKeyToFieldName				= new HashMap<String, String>();

	private IniFile						settings;
	private long						batchSize;				// How many patient records, and all related records, should be loaded before inserting batch in target
	public long							maxRowsPerPerson;		// Maximum rows of data in source tables per person
	public String						minObservationDateStr;	// Minimum observation date
	public Boolean						generateQaSamples;		// Generate QA Samples at end of ETL
	public double						qaSampleProbability;	// The sample probability value used to include a patient's records in the QA Sample export
	public String						extractionDateStr;		// Extraction date
	public String						registrationDateStr;	// Registration date
	
	private String 						sqlSourceToConcept;

	public class Concept {
		public String	conceptCode;
		public Long		conceptId;
		public String	conceptName;
		public String	vocabularyId;
		public String	domainId;
		public String	standard;
		public String	startDate;
		public String	endDate;
		public String	invalidReason;
	}
	public class SourceToConcept {
		public Concept	source;
		public Concept	target;
	}

	// ******************** Process ********************//
	public void process(String folder, DbSettings dbSettings, int maxPersons, int versionId) {
		this.folder = folder;
		patientKeyToFieldName.clear();
		cancerKeyToFieldName.clear();
		deathKeyToFieldName.clear();
		drugKeyToFieldName.clear();

		loadSettings();

		extractionDate = StringUtilities.databaseTimeStringToDays(extractionDateStr);
		registrationDate = StringUtilities.databaseTimeStringToDays(registrationDateStr);
		System.out.println("Extraction date is set at " + extractionDateStr);
		System.out.println("Registration date is set at " + registrationDateStr);

		//		checkTablesForFormattingErrors();  //TODO: FIX ERROR!!!
		sortTables();
		connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		connection.setContext(this.getClass());
		connection.use(dbSettings.database);
		loadMappings(dbSettings);

		String ffName = "field_mappings.csv";
		File ff = new File(ffName);
		if(ff.exists() && !ff.isDirectory()) { 
			System.out.println("- Loading custom field mappings from field_mappings.csv");
			for (Row row : new ReadCSVFileWithHeader(ffName)) {
				insertUpdateFieldMappingRow(row);
			}

		}

		List<String> tableNames = connection.getTableNames(dbSettings.database);

		if (!tableNames.contains("cost"))
			createCostTable(dbSettings);
		if (!tableNames.contains("_version"))
			connection.execute("CREATE TABLE _version(version_id INT,version_date DATE);");	

		truncateTables(connection);

		String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
		connection.execute("INSERT INTO _version (version_id, version_date) VALUES (" + versionId + ", '" + date + "')");

		sqlSourceToConcept = loadSQL(this.getClass().getResourceAsStream("sql/sourceToConcept.sql"));

		personCount = 0;
		personId = 0;
		observationPeriodId = 0;
		drugExposureId = 0;
		conditionOccurrenceId = 0;
		visitOccurrenceId = 0;
		procedureOccurrenceId = 0;
		observationId = 0;
		measurementId = 0;
		providerId = 0;
		sourceToProviderId.clear();
		sourceToVisitId.clear();
		sourceToPersonId.clear();

		if (generateQaSamples)
			qcSampleConstructor = new QCSampleConstructor(folder + "/sample", qaSampleProbability);//0.0001);
		tableToRows = new OneToManyList<String, Row>();
		etlReport = new EtlReport(folder);
		s2t = new SourceToTargetTrack(folder);

		StringUtilities.outputWithTime("Populating CDM_Source table");
		populateCdmSourceTable();

		StringUtilities.outputWithTime("Processing patient records");
		MultiRowIterator iterator = constructMultiRowIterator();
		while (iterator.hasNext()) {
			processSourceRecords(iterator.next());

			if (personCount == maxPersons) {
				System.out.println("Reached limit of " + maxPersons + " persons, terminating");
				break;
			}
			if (personCount % batchSize == 0) {
				insertBatch();
				s2t.writeBatch();
				System.out.println("Processed " + personCount + " persons");
			}
		}
		insertBatch();
		s2t.finalize();
		System.out.println("Processed " + personCount + " persons");
		if (generateQaSamples)
			qcSampleConstructor.addCdmData(connection, dbSettings.database);

		//		String etlReportName = etlReport.generateETLReport(icd9ProcToConcept, icd9ToConcept, icd10ToConcept, atcToConcept);
		String etlReportName = etlReport.generateETLReport(icd10ToConcept, atcToConcept);
		System.out.println("An ETL report was generated and written to :" + etlReportName);

		if (etlReport.getTotalProblemCount() > 0) {
			String etlProblemListname = etlReport.generateProblemReport();
			System.out.println("An ETL problem list was generated and written to :" + etlProblemListname);
		}
		StringUtilities.outputWithTime("Finished ETL");
	}

	//********************  ********************//
	private void populateCdmSourceTable() {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		connection.executeResource("sql/PopulateCdmSourceTable.sql", "@today", df.format(new Date()));
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
	private void checkTablesForFormattingErrors() {
		StringUtilities.outputWithTime("Checking tables for formatting errors");
		CSVFileChecker checker = new CSVFileChecker();
		checker.setFrame(ObjectExchange.frame);
		checker.checkSpecifiedFields(folder, new ReadCSVFileWithHeader(this.getClass().getResourceAsStream("csv/FieldsV5.csv")).iterator());
		StringUtilities.outputWithTime("Finished checking tables");
	}

	//********************  ********************//
	private void sortTables() {
		StringUtilities.outputWithTime("Sorting tables");
		for (String table : TABLES) {
			StringUtilities.outputWithTime("- Sorting " + table);
			FileSorter.sort(folder + "/" + table + ".csv", "lopnr");
		}
		StringUtilities.outputWithTime("Finished sorting");
	}

	//********************  ********************//
	private void insertBatch() {
		removeRowsWithNonNullableNulls();
//		removeRowsOutsideOfObservationTime();

		etlReport.registerOutgoingData(tableToRows);
		for (String table : tableToRows.keySet())
			connection.insertIntoTable(tableToRows.get(table).iterator(), table, false, true);
		tableToRows.clear();
	}
	//********************  ********************//
	private void insertUpdateFieldMappingRow(Row row) {
		row.upperCaseFieldNames();
		String srcFile = row.get("FILE");
		String srcKey = row.get("KEY").toLowerCase();
		String srcField = row.get("FIELD");
		switch (srcFile) {
		case "patient":
			patientKeyToFieldName.put(srcKey, srcField);
			break;
		case "cancer":
			cancerKeyToFieldName.put(srcKey, srcField);
			break;
		case "death":
			deathKeyToFieldName.put(srcKey, srcField);
			break;
		case "drug":
			drugKeyToFieldName.put(srcKey, srcField);
			break;
		}
	}
	//********************  ********************//
	private void loadMappings(DbSettings dbSettings) {
		StringUtilities.outputWithTime("Loading mappings from server");
		RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		connection.setContext(this.getClass());
		connection.use(dbSettings.database);

		System.out.println("- Loading field mapping");
		for (Row row : new ReadCSVFileWithHeader(this.getClass().getResourceAsStream("csv/fields.csv"))) {
			insertUpdateFieldMappingRow(row);
		}
		
		System.out.println("- Loading atc to concept_id mapping");
		atcToConcept = new CodeToDomainConceptMap("atc to concept_id mapping", "Drug");
		for (Row row : connection.queryResource("sql/atcToRxNorm.sql")) {
			row.upperCaseFieldNames();
			atcToConcept.add(row.get("SOURCE_CODE"), row.get("SOURCE_NAME"), row.getInt("SOURCE_CONCEPT_ID"), row.getInt("TARGET_CONCEPT_ID"),
					row.get("TARGET_CODE"), row.get("TARGET_NAME"), row.get("DOMAIN_ID"));
		}

		System.out.println("- Loading icd-10 to concept_id mapping");
		icd10ToConcept = new CodeToDomainConceptMap("icd-10 to concept_id mapping", "Condition");
		for (Row row : connection.queryResource("sql/icd10ToConditionProcMeasObsDevice.sql")) {
			row.upperCaseFieldNames();
			icd10ToConcept.add(row.get("SOURCE_CODE"), row.get("SOURCE_NAME"), row.getInt("SOURCE_CONCEPT_ID"), row.getInt("TARGET_CONCEPT_ID"),
					row.get("TARGET_CODE"), row.get("TARGET_NAME"), row.get("DOMAIN_ID"));
		}

		StringUtilities.outputWithTime("Finished loading mappings");
	}

	//********************  ********************//
	private void loadSettings() {
		batchSize = BATCH_SIZE_DEFAULT;
		maxRowsPerPerson = MAX_ROWS_PER_PERSON_DEFAULT;
		minObservationDateStr = MIN_OBSERVATION_DATE_DEFAULT;
		generateQaSamples = GENERATE_QASAMPLES_DEFAULT;
		qaSampleProbability = QA_SAMPLE_PROBABILITY_DEFAULT;
		extractionDateStr = EXTRACTION_DATE_DEFAULT;
		registrationDateStr = REGISTRATION_DATE_DEFAULT;
		
		File f = new File("JACSE.ini");
		if(f.exists() && !f.isDirectory()) { 
			System.out.println("- Loading custom settings from JACSE.ini");
			try {
				settings = new IniFile("JACSE.ini");
				try {
					Long numParam = Long.valueOf(settings.get("batchSize"));
					if (numParam != null)
						batchSize = numParam;
				} catch (Exception e) {
					batchSize = BATCH_SIZE_DEFAULT;
				}

				try {
					Long numParam = Long.valueOf(settings.get("maxRowsPerPerson"));
					if (numParam != null)
						maxRowsPerPerson = numParam;
				} catch (Exception e) {
					maxRowsPerPerson = MAX_ROWS_PER_PERSON_DEFAULT;
				}

				try {
					String strParam = settings.get("minObservationDate");
					if ((strParam != null) && (CSVFileChecker.isDateFormat1(strParam)))
						minObservationDateStr = strParam;
					else
						minObservationDateStr = MIN_OBSERVATION_DATE_DEFAULT;
				} catch (Exception e) {
					minObservationDateStr = MIN_OBSERVATION_DATE_DEFAULT;
				}

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

				try {
					String strParam = settings.get("extractionDate");
					if ((strParam != null) && (CSVFileChecker.isDateFormat1(strParam)))
						extractionDateStr = strParam;
					else
						extractionDateStr = EXTRACTION_DATE_DEFAULT;
				} catch (Exception e) {
					extractionDateStr = EXTRACTION_DATE_DEFAULT;
				}

				try {
					String strParam = settings.get("registrationDate");
					if ((strParam != null) && (CSVFileChecker.isDateFormat1(strParam)))
						registrationDateStr = strParam;
					else
						registrationDateStr = REGISTRATION_DATE_DEFAULT;
				} catch (Exception e) {
					registrationDateStr = REGISTRATION_DATE_DEFAULT;
				}

				
			} catch (Exception e) {
				StringUtilities.outputWithTime("No .ini file found");
			}
		}
	}

	//********************  ********************//
	private MultiRowIterator constructMultiRowIterator() {
		@SuppressWarnings("unchecked")
		Iterator<Row>[] iterators = new Iterator[TABLES.length];
		for (int i = 0; i < TABLES.length; i++)
			iterators[i] = new ReadCSVFileWithHeader(folder + "/" + TABLES[i] + ".csv").iterator();
		return new MultiRowIterator("lopnr", TABLES, iterators);
	}

	//********************  ********************//
	private void fixDateProblem(MultiRowSet patientData) {
		for (List<Row> rows : patientData.values())
			if (rows.size() != 0) {
				List<String> dateFields = new ArrayList<String>();
				for (String fieldName : rows.get(0).getFieldNames())
					if (fieldName.toLowerCase().startsWith("date") || fieldName.toLowerCase().endsWith("date"))
						dateFields.add(fieldName);
				for (Row row : rows)
					for (String fieldName : dateFields) {
						String value = row.get(fieldName);
						if (CSVFileChecker.isDateFormat1(value))
							row.set(fieldName, value.substring(0, 10));
					}
			}
	}

	//******************** processSourceRecords ********************//
	private void processSourceRecords(MultiRowSet patientData) {
		etlReport.registerIncomingData(patientData);

		if (patientData.linkingId.trim().length() == 0) {
			for (String table : patientData.keySet())
				if (patientData.get(table).size() > 0)
					etlReport.reportProblem(table, "Missing person_id in " + patientData.get(table).size() + " rows in table " + table, "");
			return;
		}

		if (patientData.totalSize() > maxRowsPerPerson) {
			etlReport.reportProblem("", "Person has too many rows (" + patientData.totalSize() + "). Skipping person", patientData.linkingId);
			return;
		}

		if (generateQaSamples)
			qcSampleConstructor.registerPersonData(patientData, personId);
		fixDateProblem(patientData);

		processPatient(patientData);
		processCancer(patientData);
		processDrug(patientData);
		processDeath(patientData);
	}

	//******************** CANCER ********************//
	private void processCancer(MultiRowSet patientData) {
		for (Row row : patientData.get("CANCER")) {
			String lopnr = row.get(getField("cancer","lopnr"));
			Long pId = sourceToPersonId.get(lopnr);
			String startDate = formatDateFrom8To10(row.get(getField("cancer","diadatn")));
			String visitRef = createVisitRef(lopnr, startDate);
			Long visitId = sourceToVisitId.get(visitRef);
			if (visitId == null) {
				visitId = addToVisitOccurrence(
						pId,				// person_id
						startDate, 			// visit_start_date
						startDate, 			// visit_end_date
						9202,				// visit_concept_id, 9202: Outpatient visit 
						(long) 44818518, 	// visit_type_concept_id, 44818518: Visit derived from EHR record
						null, 				// care_site_id
						null, 				// provider_id
						visitRef);			// visit_source_value
				s2t.add("", "cancer.csv", lopnr, startDate, "", null, "visit_occurrence", visitId.toString(), startDate, "Visit", "", (long) 9202);
				sourceToVisitId.put(visitRef,  visitId);
			}
			Long condOccId = null;
			SourceToConcept s2c = null;
			String icdLookup = null;
			String icdValue = null;

			for (String icdField : ICD_FIELDS) {
				icdValue = row.get(getField("cancer",icdField));
				if (icdValue.length() > 0) {
					s2c = lookupConceptId(icdField, icdValue);
					icdLookup = icdField;
					if ((s2c != null) && (s2c.source != null))
						break;
				}
			}
			if (s2c != null) {
				if (s2c.target != null) {
					String targetDomain = s2c.target.domainId;
					switch (targetDomain) {
					case "Condition":
						condOccId = addToConditionOccurrence(
								pId,							// person_id
								s2c.target != null ? s2c.target.conceptId : 0,	// condition_concept_id
										startDate,				// condition_start_date
										null,					// condition_end_date
										(long) 44786627, 		// condition_type_concept_id, 44786627: Primary Condition,
										null,					// stop_reason
										null,					// provider_id
										visitId,				// visit_occurrence_id
										s2c.source.conceptCode,	// condition_source_value
										s2c.source.conceptId);	// condition_source_concept_id
						s2t.add("", "cancer.csv", s2c.source.conceptCode, s2c.source.conceptName, s2c.source.vocabularyId, s2c.source.conceptId, 
								"condition_occurrence", s2c.target != null ? s2c.target.conceptCode : "", s2c.target != null ? s2c.target.conceptName : "", 
										s2c.target != null ? s2c.target.domainId : "", s2c.target != null ? s2c.target.vocabularyId : "", s2c.target != null ? s2c.target.conceptId : (long) 0);
						break;
					case "Observation":
						long obsId = addToObservation(
								personId, 					// person_id
								(long) s2c.target.conceptId,// observation_concept_id
								startDate, 					// observation_date
								(long) 38000280, 			// observation_type_concept_id, 38000280: Observation recorded from EHR
								(long) s2c.target.conceptId,// value_as_concept_id
								null, 						// unit_concept_id
								null, 						// provider_id
								null, 						// visit_occurrence_id
								s2c.source.conceptCode, 	// observation_source_vale
								(long) s2c.source.conceptId,// observation_source_concept_id
								"");						// unit_source_value
						s2t.add("", "cancer.csv", s2c.source.conceptCode, s2c.source.conceptName, s2c.source.vocabularyId, s2c.source.conceptId, 
								"condition_occurrence", s2c.target != null ? s2c.target.conceptCode : "", s2c.target != null ? s2c.target.conceptName : "", 
										s2c.target != null ? s2c.target.domainId : "", s2c.target != null ? s2c.target.vocabularyId : "", s2c.target != null ? s2c.target.conceptId : (long) 0);
						break;
					default:
						System.out.println("Other domain from CANCER:"+targetDomain+", "+s2c.source.conceptCode+", "+ (s2c.target != null ? s2c.target.conceptId.toString() : "") +", "+personId); 
						break;
					}
				}
				else { // No target found
					condOccId = addToConditionOccurrence(
							pId,					// person_id
							0,						// condition_concept_id
							startDate,				// condition_start_date
							null,					// condition_end_date
							(long) 44786627, 		// condition_type_concept_id, 44786627: Primary Condition,
							null,					// stop_reason
							null,					// provider_id
							visitId,				// visit_occurrence_id
							s2c.source.conceptCode,	// condition_source_value
							s2c.source.conceptId);	// condition_source_concept_id
					s2t.add("", "cancer.csv", s2c.source.conceptCode, s2c.source.conceptName, s2c.source.vocabularyId, s2c.source.conceptId, 
							"condition_occurrence", "", "", "", "", (long) 0);
				}
			}
			else { // No lookup matches at all
				condOccId = addToConditionOccurrence(
						pId,				// person_id
						0,					// condition_concept_id
						startDate,			// condition_start_date
						null,				// condition_end_date
						(long) 44786627, 	// condition_type_concept_id, 44786627: Primary Condition,
						null,				// stop_reason
						null,				// provider_id
						visitId,			// visit_occurrence_id
						icdValue,			// condition_source_value
						(long) 0);			// condition_source_concept_id
				s2t.add("", "cancer.csv", icdValue, "", icdLookup != null ? icdLookup : "", null, 
						"condition_occurrence", "", "",	"", "", (long) 0);
			}
		}
	}

	//******************** DRUG EXPOSURE ********************//
	private void processDrug(MultiRowSet patientData) {
		for (Row row : patientData.get("DRUG")) {
			String expDate = row.get(getField("drug","edatum"));
			String prescriptionDate = row.get(getField("drug","fdatum"));
			String lopnr = row.get(getField("drug","lopnr"));
			Long pId = sourceToPersonId.get(lopnr);
			String atc = row.get(getField("drug","atc"));

			String visitRef = createVisitRef(lopnr, prescriptionDate);
			Long visitId = sourceToVisitId.get(visitRef);
			if (visitId == null) {
				visitId = addToVisitOccurrence(
						pId,				// person_id
						prescriptionDate, 	// visit_start_date
						prescriptionDate, 	// visit_end_date
						9202,				// visit_concept_id, 9202: Outpatient visit 
						(long) 44818518, 	// visit_type_concept_id, 44818518: Visit derived from EHR record
						null, 				// care_site_id
						null, 				// provider_id
						visitRef);			// visit_source_value
				s2t.add("", "drug.csv", lopnr, prescriptionDate, "", null, "visit_occurrence", visitId.toString(), prescriptionDate, "Visit", "", (long) 9202);
				sourceToVisitId.put(visitRef,  visitId);
			}
			Integer numPacks = null;
			Integer refills = null;
			String tmpS2 = row.get(getField("drug","antal"));
			if ((tmpS2 != null) && (!tmpS2.equals(""))) {
				numPacks = Double.valueOf(tmpS2).intValue();
				if (numPacks > 1)
					refills = numPacks - 1;
			}

			Integer forDDD = null;
			tmpS2 = row.get(getField("drug","forpddd"));
			if ((tmpS2 != null) && (!tmpS2.equals("")))
				forDDD = Double.valueOf(tmpS2).intValue();

			Integer packSize = null;
			tmpS2 = row.get(getField("drug","antnum"));
			if ((tmpS2 != null) && (!tmpS2.equals("")))
				packSize = Double.valueOf(tmpS2).intValue();

			CodeDomainData data = atcToConcept.getCodeData(row.get(getField("drug","atc")));
			for (TargetConcept targetConcept : data.targetConcepts) {
				String targetDomain = targetConcept.domainId;
				switch (targetDomain) {
				case "Drug":
					addToDrugExposure(
							personId,					// person_id
							targetConcept.conceptId,	// drug_concept_id
							expDate, 					// drug_exposure_start_date
							expDate,						// drug_exposure_end_date
							38000175, 					// drug_type_concept_id, 38000175: Prescription dispensed in pharmacy
							refills,					// refills
							(double) numPacks,			// quantity
							forDDD,						// days_supply
							visitId,					// visit_occurrence_id
							atc,						// drug_source_value
							data.sourceConceptId);		// drug_source_concept_id
					s2t.add("", "drug.csv", atc, data.sourceConceptName, "atc", (long) data.sourceConceptId, "drug_exposure", targetConcept.conceptCode, targetConcept.conceptName, targetConcept.domainId, "RxNorm", (long) targetConcept.conceptId); 
					break;
				default:
					System.out.println("Other domain from DRUGS:"+targetDomain+", "+row.get(getField("drug","atc"))+", "+targetConcept.conceptId+", "+personId); 
					break;
				}
			}
		}

	}


	//******************** DEATH ********************//

	private void processDeath(MultiRowSet patientData) {
		for (Row row : patientData.get("DEATH")) {
			String dateOfDeath = row.get(getField("death","dodsdatn"));
			if (dateOfDeath.trim().length() != 0) {
				String causeOfDeathCode = row.get(getField("death","ulorsak"));
				String icdVersion = row.get(getField("death","icd"));
				CodeDomainData causeData = getCauseOfDeathConcept(causeOfDeathCode, icdVersion);
				Integer causeConceptId = 0;
				if (causeData.targetConcepts != null)
					causeConceptId = causeData.targetConcepts.get(0).conceptId;
				Row death = new Row();
				death.add("person_id", personId);
				death.add("death_type_concept_id", 38003569);  // 38003569: EHR record patient status "Deceased"
				death.add("death_date", dateOfDeath);
				death.add("cause_concept_id", causeConceptId); 
				death.add("cause_source_value", causeOfDeathCode);
				death.add("cause_source_concept_id", causeData.sourceConceptId);
				tableToRows.put("death", death);

				if (causeData.targetConcepts != null)
					s2t.add("", "death.csv", causeOfDeathCode, causeData.sourceConceptName, icdVersion, (long) causeData.sourceConceptId, 
							"death", causeData.targetConcepts.get(0).conceptCode, causeData.targetConcepts.get(0).conceptName, 
							causeData.targetConcepts.get(0).domainId, icdVersion, (long) causeData.targetConcepts.get(0).conceptId);
				else
					s2t.add("", "death.csv", causeOfDeathCode, causeData.sourceConceptName, icdVersion, (long) causeData.sourceConceptId, 
							"death", "", "", "", icdVersion, (long) 0);

				// Record contributing causes as CONDITION_OCCURRENCE as well
				String morsakBase = getField("death","morsak1").substring(0, getField("death","morsak1").length() - 1);
				for (int morsakNum = 0; morsakNum < 46; morsakNum++) {
					String morsak = morsakBase + Integer.toString(morsakNum + 1);
					String morsakVal = row.get(morsak);

					if ((morsakVal != null) && (morsakVal.length() > 0)) {
						CodeDomainData contribData = getCauseOfDeathConcept(morsakVal, icdVersion);
						if (contribData.targetConcepts != null) {
							for (TargetConcept targetConcept : contribData.targetConcepts) {
								String targetDomain = targetConcept.domainId;

								switch (targetDomain) {
								case "Condition":
									long condId = addToConditionOccurrence(
											personId, 								// person_id
											(long) targetConcept.conceptId, 		// condition_concept_id
											dateOfDeath, 							// condition_start_date
											dateOfDeath, 							// condition_end_date
											(long) 44786629, 						// condition_type_concept_id, 44786629: Secondary Condition
											null, 									// stop_reason
											null, 									// provider_id
											null, 									// visit_occurrence_id
											morsakVal, 								// condition_source_value
											(long) contribData.sourceConceptId);	// condition_source_concept_id
									s2t.add("", "death.csv", morsakVal, contribData.sourceConceptName, icdVersion, (long) contribData.sourceConceptId, 
											"condition_occurrence", targetConcept.conceptCode, targetConcept.conceptName, 
											targetConcept.domainId, "", (long) targetConcept.conceptId);
									break;
								case "Observation":
									long obsId = addToObservation(
											personId, 							// person_id
											(long) targetConcept.conceptId, 	// observation_concept_id
											dateOfDeath, 						// observation_date
											(long) 38000280, 					// observation_type_concept_id, 38000280: Observation recorded from EHR
											(long) targetConcept.conceptId, 	// value_as_concept_id
											null, 								// unit_concept_id
											null, 								// provider_id
											null, 								// visit_occurrence_id
											morsakVal, 							// observation_source_vale
											(long) contribData.sourceConceptId, // observation_source_concept_id
											"");								// unit_source_value
									s2t.add("", "death.csv", morsakVal, contribData.sourceConceptName, icdVersion, (long) contribData.sourceConceptId, 
											"observation", targetConcept.conceptCode, targetConcept.conceptName, 
											targetConcept.domainId, "", (long) targetConcept.conceptId);
									break;
								default:
									System.out.println("Other domain from death:"+targetDomain+", "+causeOfDeathCode+", "+targetConcept.conceptId+", "+personId); 
									break;
								}
							}
						}
						else
						{
							long condId = addToConditionOccurrence(
									personId, 								// person_id
									(long) 0, 								// condition_concept_id
									dateOfDeath, 							// condition_start_date
									dateOfDeath, 							// condition_end_date
									(long) 44786629, 						// condition_type_concept_id, 44786629: Secondary Condition
									null, 									// stop_reason
									null, 									// provider_id
									null, 									// visit_occurrence_id
									morsakVal, 								// condition_source_value
									(long) contribData.sourceConceptId);	// condition_source_concept_id
							s2t.add("", "death.csv", morsakVal, contribData.sourceConceptName, icdVersion, (long) contribData.sourceConceptId, 
									"condition_occurrence", "", "", "condition_occurrence", "", (long) 0);
						}
					}
				}
			}
		}
	}

	//******************** PERSON ********************//

	private void processPatient(MultiRowSet patientData) {
		for (Row row : patientData.get("PATIENT")) {
			String lopnr = row.get(getField("patient","lopnr"));

			if (sourceToPersonId.get(lopnr) == null) { // Did not encounter this person before
				personCount++;
				personId++;
				sourceToPersonId.put(lopnr, personId);

				String deathDate = null;
				List<Row> deathRows = patientData.get("DEATH");
				if (deathRows.size() > 0) {
					Row death = deathRows.get(0);
					if (deathRows.size() > 1) { // Multiple rows: find the one with the latest startdate:
						RowUtilities.sort(deathRows);
						death = deathRows.get(deathRows.size() - 1);
					}
					deathDate = death.get(getField("death","dodsdatn"));
				}
				
				Row person = new Row();
				person.add("person_id", personId);
				String gender = row.get(getField("patient","kon"));
				person.add("gender_concept_id", gender.toLowerCase().equals("1") ? "8507" : gender.toLowerCase().equals("2") ? "8532" : "8551");
				String dateOfBirth = row.get(getField("patient","foddat"));
				if (dateOfBirth.length() < 10) {
					person.add("year_of_birth", "");
					person.add("month_of_birth", "");
					person.add("day_of_birth", "");
					dateOfBirth = null;
				} else { 
					String yearStr = dateOfBirth.substring(0, 4);
					String monthStr = dateOfBirth.substring(5, 7);
					String dayStr = dateOfBirth.substring(8, 10);
					if (isInteger(yearStr)) {
						person.add("year_of_birth", yearStr);
						if (isInteger(monthStr)) {
							person.add("month_of_birth", monthStr);
							if (isInteger(dayStr)) {
								person.add("day_of_birth", dayStr);
								person.add("birth_datetime", dateOfBirth);
							} else {
								person.add("day_of_birth", ""); 
								person.add("birth_datetime", "");
							}
						}
						else {
							person.add("month_of_birth", "");
							person.add("day_of_birth", "");
							person.add("birth_datetime", "");
						}
					}
					else {
						person.add("year_of_birth", "");
						person.add("month_of_birth", "");
						person.add("day_of_birth", "");
						person.add("birth_datetime", "");
					}
				}
				person.add("race_concept_id", 8552); // Unknown
				person.add("ethnicity_concept_id", 0); // Unknown

				person.add("person_source_value", lopnr);
				person.add("gender_source_value", gender);
				person.add("provider_id", "");
				tableToRows.put("person", person);
				s2t.add("", "cancer.csv", lopnr, "", "", null, "person", Long.toString(personId), "", "", "", null);

				String obsStartDate = null;
				if (dateOfBirth == null)
					obsStartDate = registrationDateStr;
				else {
					if (StringUtilities.databaseTimeStringToDays(dateOfBirth) > registrationDate)
						obsStartDate = dateOfBirth;
					else
						obsStartDate = registrationDateStr;
				}
				
				String obsEndDate = null;
				if (deathDate != null)
					obsEndDate = deathDate;
				else
					obsEndDate = extractionDateStr;

				Row observationPeriod  = new Row();
				observationPeriod.add("observation_period_id", personId);
				observationPeriod.add("person_id", personId);
				observationPeriod.add("observation_period_start_date", obsStartDate);
				observationPeriod.add("observation_period_start_datetime", obsStartDate);
				observationPeriod.add("observation_period_end_date", obsEndDate);
				observationPeriod.add("observation_period_end_datetime", obsEndDate);
				observationPeriod.add("period_type_concept_id", 44814724); // 44814724: Period covering healthcare encounters
				tableToRows.put("observation_period", observationPeriod);
				s2t.add("", "cancer.csv", lopnr, "", "", null, "observation_period", Long.toString(personId), "", "", "", null);
			} else {
				personId = sourceToPersonId.get(lopnr);
			}
		}

	}

	//********************************************************************************//

	private SourceToConcept lookupConceptId(String icdVocab, String icdValue) {
		SourceToConcept retConcept = null;
		String vocab = null;
		switch (icdVocab) {
		case "ICD-O/3":
			vocab = "ICDO3";
			break;
		case "ICD-O/2-10":
			vocab = "ICD10CM";
			break;
		case "ICD-9":
			vocab = "ICD9CM";
			break;
		case "ICD-7":
			vocab = "ICD9CM";
			break;
		default:
			break;
		}
		if (vocab != null) {
			String sql = sqlSourceToConcept.replace("__SOURCEVOCAB__", vocab).replace("__SOURCECODE__", icdValue);

			for (Row row : connection.query(sql)) {
				row.upperCaseFieldNames();
				retConcept = new SourceToConcept();
				retConcept.source = new Concept();
				retConcept.source.conceptCode = icdValue;
				retConcept.source.conceptId = row.getLong("SOURCE_CONCEPT_ID");
				retConcept.source.conceptName = row.get("SOURCE_NAME");
				retConcept.source.vocabularyId = row.get("SOURCE_VOCABULARY_ID");
				retConcept.source.domainId = row.get("SOURCE_DOMAIN_ID");
				retConcept.target = new Concept();
				retConcept.target.conceptCode = row.get("TARGET_CODE");
				retConcept.target.conceptId = row.getLong("TARGET_CONCEPT_ID");
				retConcept.target.conceptName = row.get("TARGET_NAME");
				retConcept.target.vocabularyId = row.get("TARGET_VOCABULARY_ID");
				retConcept.target.domainId = row.get("TARGET_DOMAIN_ID");
				retConcept.target.standard = row.get("STANDARD");
				retConcept.target.startDate = row.get("TARGET_VALID_START_DATE");
				retConcept.target.endDate = row.get("TARGET_VALID_END_DATE");
				retConcept.target.invalidReason = row.get("TARGET_INVALID_REASON");
				break;
			}
			if (retConcept == null) {
				String sqlSrc = "SELECT concept_code, concept_id, concept_name, vocabulary_id, domain_id from concept where vocabulary_id='";
				sqlSrc += vocab + "' and replace(concept_code,'.','')='" + icdValue +"';";	
				for (Row row : connection.query(sqlSrc)) {
					row.upperCaseFieldNames();
					retConcept = new SourceToConcept();
					retConcept.source = new Concept();
					retConcept.source.conceptCode = icdValue;
					retConcept.source.conceptId = row.getLong("CONCEPT_ID");
					retConcept.source.conceptName = row.get("CONCEPT_NAME");
					retConcept.source.vocabularyId = row.get("VOCABULARY_ID");
					retConcept.source.domainId = row.get("DOMAIN_ID");
					break;
				}
			}
		}
		return retConcept;
	}

	//********************  ********************//
	private String loadSQL(InputStream sqlStream) {
		StringBuilder sql = new StringBuilder();
		for (String line : new ReadTextFile(sqlStream)) {
			line = line.replaceAll("--.*", ""); // Remove comments
			line = line.trim();
			if (line.length() != 0) {
				sql.append(line.trim());
				sql.append('\n');
			}
		}
		return sql.toString();
	}

	//********************  ********************//
	private long addToMeasurement(
			long mPersonId,
			long measurementConceptId,
			String measurementDate,
			long measurementTypeConceptId,
			Double valueAsNumber,
			Long valueAsConceptId,
			Long unitConceptId,
			Long mProviderId,
			String measurementSourceValue,
			String unitSourceValue,
			String valueSourceValue) {
		//		addToMeasurement(personId, measurementConceptId, measurementDate, measurementTime, measurementTypeConceptId, operatorConceptId, valueAsNumber, valueAsConceptId, unitConceptId, rangeLow, rangeHigh, providerId, visitOccurrenceId, measurementSourceValue, measurementSourceConceptId, unitSourceValue, valueSourceValue);
		return addToMeasurement(mPersonId, measurementConceptId, measurementDate, null, measurementTypeConceptId, null, valueAsNumber, valueAsConceptId, unitConceptId, null, null, mProviderId, null, measurementSourceValue, null, unitSourceValue, valueSourceValue);
	}

	//********************  ********************//
	private long addToMeasurement(
			long mPersonId,
			long measurementConceptId,
			String measurementDate,
			String measurementDatetime,
			long measurementTypeConceptId,
			Long operatorConceptId,
			Double valueAsNumber,
			Long valueAsConceptId,
			Long unitConceptId,
			Double rangeLow,
			Double rangeHigh,
			Long mProviderId,
			Long visitOccurrenceId,
			String measurementSourceValue,
			Long measurementSourceConceptId,
			String unitSourceValue,
			String valueSourceValue) {
		Row measurement = new Row();
		measurement.add("measurement_id", ++measurementId);
		long retId = measurementId;
		measurement.add("person_id", mPersonId);
		measurement.add("measurement_concept_id", measurementConceptId);
		measurement.add("measurement_date", (measurementDate != null ? measurementDate : ""));
		measurement.add("measurement_datetime", (measurementDatetime != null ? measurementDatetime : ""));
		measurement.add("measurement_type_concept_id", measurementTypeConceptId);
		measurement.add("operator_concept_id", (operatorConceptId != null ? operatorConceptId.toString() : ""));
		measurement.add("value_as_number", (valueAsNumber != null ? valueAsNumber.toString() : ""));
		measurement.add("value_as_concept_id", (valueAsConceptId != null ? valueAsConceptId.toString() : ""));
		measurement.add("unit_concept_id", (unitConceptId != null ? unitConceptId.toString() : ""));
		measurement.add("range_low", (rangeLow != null ? rangeLow.toString() : ""));
		measurement.add("range_high", (rangeHigh != null ? rangeHigh.toString() : ""));
		measurement.add("provider_id", (mProviderId != null ? mProviderId.toString() : ""));
		measurement.add("visit_occurrence_id", (visitOccurrenceId != null ? visitOccurrenceId.toString() : ""));
		measurement.add("measurement_source_value", measurementSourceValue != null ? measurementSourceValue : "");
		measurement.add("measurement_source_concept_id", (measurementSourceConceptId != null ? measurementSourceConceptId.toString() : ""));
		measurement.add("unit_source_value", unitSourceValue != null ? unitSourceValue : "");
		measurement.add("value_source_value", valueSourceValue != null ? valueSourceValue : "");
		tableToRows.put("measurement", measurement);
		return retId;
	}

	//********************  ********************//
	private long addToObservation(
			long obPersonId,
			long observationConceptId,
			String observationDate,
			long observationTypeConceptId,
			Double valueAsNumber,
			Long unitConceptId,
			Long obProviderId,
			Long visitOccurrenceId,
			String observationSourceValue,
			Long observationSourceConceptId,
			String unitSourceValue) {
		return addToObservation(obPersonId,observationConceptId,observationDate,null,observationTypeConceptId,valueAsNumber,
				null,null,null,unitConceptId,obProviderId,visitOccurrenceId,observationSourceValue,
				observationSourceConceptId,unitSourceValue,null);
	}

	//********************  ********************//
	private long addToObservation(
			long obPersonId,
			long observationConceptId,
			String observationDate,
			long observationTypeConceptId,
			String valueAsString,
			Long unitConceptId,
			Long obProviderId,
			Long visitOccurrenceId,
			String observationSourceValue,
			Long observationSourceConceptId,
			String unitSourceValue) {
		return addToObservation(obPersonId,observationConceptId,observationDate,null,observationTypeConceptId,null,
				valueAsString,null,null,unitConceptId,obProviderId,visitOccurrenceId,observationSourceValue,
				observationSourceConceptId,unitSourceValue,null);
	}

	//********************  ********************//
	private long addToObservation(
			long obPersonId,
			long observationConceptId,
			String observationDate,
			long observationTypeConceptId,
			Long valueAsConceptId,
			Long unitConceptId,
			Long obProviderId,
			Long visitOccurrenceId,
			String observationSourceValue,
			Long observationSourceConceptId,
			String unitSourceValue) {
		return addToObservation(obPersonId,observationConceptId,observationDate,null,observationTypeConceptId,null,
				null,valueAsConceptId,null,unitConceptId,obProviderId,visitOccurrenceId,observationSourceValue,
				observationSourceConceptId,unitSourceValue,null);
	}

	//********************  ********************//
	private long addToObservation(
			long obPersonId,
			long observationConceptId,
			String observationDate,
			String observationDateTime,
			long observationTypeConceptId,
			Double valueAsNumber,
			String valueAsString,
			Long valueAsConceptId,
			Long qualifierConceptId,
			Long unitConceptId,
			Long obProviderId,
			Long visitOccurrenceId,
			String observationSourceValue,
			Long observationSourceConceptId,
			String unitSourceValue,
			String qualifierSourceValue) {
		Row observation = new Row();
		observation.add("observation_id", ++observationId);
		long retId = observationId;
		observation.add("person_id", obPersonId);
		observation.add("observation_concept_id", observationConceptId);
		observation.add("observation_date", observationDate);
		observation.add("observation_datetime", observationDateTime != null ? observationDateTime : "");
		observation.add("observation_type_concept_id", observationTypeConceptId);
		observation.add("value_as_number", (valueAsNumber != null ? valueAsNumber.toString() : ""));
		observation.add("value_as_string", valueAsString != null ? valueAsString : "");
		observation.add("value_as_concept_id", (valueAsConceptId != null ? valueAsConceptId.toString() : ""));
		observation.add("qualifier_concept_id", (qualifierConceptId != null ? qualifierConceptId.toString() : ""));
		observation.add("unit_concept_id", (unitConceptId != null ? unitConceptId.toString() : ""));
		observation.add("provider_id", (obProviderId != null ? obProviderId.toString() : ""));
		observation.add("visit_occurrence_id", (visitOccurrenceId != null ? visitOccurrenceId.toString() : ""));
		observation.add("observation_source_value", observationSourceValue != null ? observationSourceValue : "");
		observation.add("observation_source_concept_id", (observationSourceConceptId != null ? observationSourceConceptId.toString() : ""));
		observation.add("unit_source_value", unitSourceValue != null ? unitSourceValue : "");
		observation.add("qualifier_source_value", qualifierSourceValue != null ? qualifierSourceValue : "");
		tableToRows.put("observation", observation);
		return retId;
	}

	//********************  ********************//
	private long addToConditionOccurrence(
			long coPersonId,
			long conditionConceptId,
			String conditionStartDate,
			String conditionEndDate,
			Long conditionTypeConceptId,
			String stopReason,
			Long coProviderId,
			Long coVisitOccurrenceId,
			String conditionSourceValue,
			Long conditionSourceConceptId)
	{
		Row conditionOccurrence = new Row();
		long retId = conditionOccurrenceId;
		conditionOccurrence.add("condition_occurrence_id", conditionOccurrenceId++);
		conditionOccurrence.add("person_id", coPersonId);
		conditionOccurrence.add("condition_concept_id", conditionConceptId); 
		conditionOccurrence.add("condition_start_date", conditionStartDate != null ? conditionStartDate : "");
		conditionOccurrence.add("condition_start_datetime", conditionStartDate != null ? conditionStartDate : "");
		conditionOccurrence.add("condition_end_date", conditionEndDate != null ? conditionEndDate : "");
		conditionOccurrence.add("condition_end_datetime", conditionEndDate != null ? conditionEndDate : "");
		conditionOccurrence.add("condition_type_concept_id", (conditionTypeConceptId != null ? conditionTypeConceptId.toString() : "")); 
		conditionOccurrence.add("stop_reason", stopReason != null ? stopReason : "");
		conditionOccurrence.add("provider_id", (coProviderId != null ? coProviderId.toString() : ""));
		conditionOccurrence.add("visit_occurrence_id", (coVisitOccurrenceId != null ? coVisitOccurrenceId.toString() : ""));
		conditionOccurrence.add("condition_source_value", conditionSourceValue != null ? conditionSourceValue : "");
		conditionOccurrence.add("condition_source_concept_id", (conditionSourceConceptId != null ? conditionSourceConceptId.toString() : ""));
		tableToRows.put("condition_occurrence", conditionOccurrence);
		return retId;
	}

	//********************  ********************//
	private void addToDrugExposure(
			long	aPersonId,
			int		aDrugConceptId,
			String	aDrugExposureStartDate,
			String	aDrugExposureEndDate,
			int		aDrugTypeConceptId,
			String	aStopReason,
			Integer	aRefills,
			Double	aQuantity,
			Integer	aDaysSupply,
			String	aSig,
			Integer	aRouteConceptId,
			String	aLotNumber,
			Long	aProviderId,
			Long	aVisitOccurrenceId,
			String	aDrugSourceValue,
			Integer	aDrugSourceConceptId,
			String	aRouteSourceValue,
			String	aDoseUnitSourceValue) {
		Row drugExposure = new Row();
		drugExposure.add("drug_exposure_id", drugExposureId++);
		drugExposure.add("person_id", aPersonId);
		drugExposure.add("drug_concept_id", aDrugConceptId);
		if (StringUtilities.isDate(aDrugExposureStartDate)) {
			drugExposure.add("drug_exposure_start_date", aDrugExposureStartDate);
			drugExposure.add("drug_exposure_start_datetime", aDrugExposureStartDate);
		}
		else {
			drugExposure.add("drug_exposure_start_date", "");
			drugExposure.add("drug_exposure_start_datetime", "");
		}

		if ((aDrugExposureEndDate != null) && (StringUtilities.isDate(aDrugExposureEndDate)))
			drugExposure.add("drug_exposure_end_date", aDrugExposureEndDate);
		else
			drugExposure.add("drug_exposure_end_date", "");
		drugExposure.add("verbatim_end_date", "");
		drugExposure.add("drug_type_concept_id", aDrugTypeConceptId);  // 38000177: Prescription written
		drugExposure.add("stop_reason", aStopReason != null ? aStopReason : "");
		drugExposure.add("refills", (aRefills != null ? aRefills.toString() : ""));
		drugExposure.add("quantity", (aQuantity != null ? aQuantity.toString() : "")); 
		drugExposure.add("days_supply", (aDaysSupply != null ? aDaysSupply.toString() : ""));  
		drugExposure.add("sig", aSig != null ? aSig : "");
		drugExposure.add("route_concept_id", (aRouteConceptId != null ? aRouteConceptId.toString() : ""));
		drugExposure.add("lot_number", aLotNumber != null ? aLotNumber : "");
		drugExposure.add("provider_id", (aProviderId != null ? aProviderId.toString() : ""));
		drugExposure.add("visit_occurrence_id", (aVisitOccurrenceId != null ? aVisitOccurrenceId.toString() : ""));
		drugExposure.add("drug_source_value", aDrugSourceValue != null ? aDrugSourceValue : "");
		drugExposure.add("drug_source_concept_id", (aDrugSourceConceptId != null ? aDrugSourceConceptId.toString() : ""));
		drugExposure.add("route_source_value", aRouteSourceValue != null ? aRouteSourceValue : ""); 
		drugExposure.add("dose_unit_source_value", aDoseUnitSourceValue != null ? aDoseUnitSourceValue : ""); 

		tableToRows.put("drug_exposure", drugExposure);
	}
	
	//********************  ********************//
	private void addToDrugExposure(
			long	aPersonId,
			int		aDrugConceptId,
			String	aDrugExposureStartDate,
			String	aDrugExposureEndDate,
			int		aDrugTypeConceptId,
			Integer	aRefills,
			Double	aQuantity,
			Integer	aDaysSupply,
			Long	aVisitOccurrenceId,
			String	aDrugSourceValue,
			Integer	aDrugSourceConceptId) {
		addToDrugExposure(
				aPersonId,
				aDrugConceptId,
				aDrugExposureStartDate,
				aDrugExposureEndDate,
				aDrugTypeConceptId,
				null,
				aRefills,
				aQuantity,
				aDaysSupply,
				null,
				null,
				null,
				null,
				aVisitOccurrenceId,
				aDrugSourceValue,
				aDrugSourceConceptId,
				null,
				null);
	}
	
	//********************  ********************//
	private long addToProcedureOccurrence(
			long poPersonId,
			long procedureConceptId,
			String procedureDate,
			long procedureTypeConceptId,
			Long modifierConceptId,
			Long quantity,
			Long poProviderId,
			Long poVisitOccurrenceId,
			String procedureSourceValue,
			Long procedureSourceConceptId,
			String qualifierSourceValue) 
	{
		Row procedureOccurrence = new Row();
		long retId = procedureOccurrenceId;
		procedureOccurrence.add("procedure_occurrence_id", procedureOccurrenceId++);
		procedureOccurrence.add("person_id", poPersonId);
		procedureOccurrence.add("procedure_concept_id", procedureConceptId); 
		procedureOccurrence.add("procedure_date", procedureDate != null ? procedureDate : "");
		procedureOccurrence.add("procedure_type_concept_id", procedureTypeConceptId); 
		procedureOccurrence.add("modifier_concept_id", (modifierConceptId != null ? modifierConceptId.toString() : "")); 
		procedureOccurrence.add("quantity", (quantity != null ? quantity.toString() : ""));
		procedureOccurrence.add("provider_id", (poProviderId != null ? poProviderId.toString() : ""));
		procedureOccurrence.add("visit_occurrence_id", (poVisitOccurrenceId != null ? poVisitOccurrenceId.toString() : ""));
		procedureOccurrence.add("procedure_source_value", procedureSourceValue != null ? procedureSourceValue : "");
		procedureOccurrence.add("procedure_source_concept_id", (procedureSourceConceptId != null ? procedureSourceConceptId.toString() : ""));
		procedureOccurrence.add("qualifier_source_value", qualifierSourceValue != null ? qualifierSourceValue : "");
		tableToRows.put("procedure_occurrence", procedureOccurrence);
		return retId;
	}
	//********************  ********************//
	private void addToProcedureOccurrence(
			long poProcOccurId,
			long poPersonId,
			long procedureConceptId,
			String procedureDate,
			String procedureDatetime,
			long procedureTypeConceptId,
			Long modifierConceptId,
			Long quantity,
			Long poProviderId,
			Long poVisitOccurrenceId,
			String procedureSourceValue,
			Long procedureSourceConceptId,
			String qualifierSourceValue) 
	{
		Row procedureOccurrence = new Row();
		if (procedureOccurrenceId < poProcOccurId)
			procedureOccurrenceId = poProcOccurId;
		procedureOccurrence.add("procedure_occurrence_id", poProcOccurId);
		procedureOccurrence.add("person_id", poPersonId);
		procedureOccurrence.add("procedure_concept_id", procedureConceptId); 
		procedureOccurrence.add("procedure_date", procedureDate != null ? procedureDate : "");
		procedureOccurrence.add("procedure_datetime", procedureDatetime != null ? procedureDatetime : "");
		procedureOccurrence.add("procedure_type_concept_id", procedureTypeConceptId); 
		procedureOccurrence.add("modifier_concept_id", (modifierConceptId != null ? modifierConceptId.toString() : "")); 
		procedureOccurrence.add("quantity", (quantity != null ? quantity.toString() : ""));
		procedureOccurrence.add("provider_id", (poProviderId != null ? poProviderId.toString() : ""));
		procedureOccurrence.add("visit_occurrence_id", (poVisitOccurrenceId != null ? poVisitOccurrenceId.toString() : ""));
		procedureOccurrence.add("procedure_source_value", procedureSourceValue != null ? procedureSourceValue : "");
		procedureOccurrence.add("procedure_source_concept_id", (procedureSourceConceptId != null ? procedureSourceConceptId.toString() : ""));
		procedureOccurrence.add("qualifier_source_value", qualifierSourceValue != null ? qualifierSourceValue : "");
		tableToRows.put("procedure_occurrence", procedureOccurrence);
	}

	//********************  ********************//
	private long addToVisitOccurrence(
			long persId,
			String visitStartDate, 
			String visitEndDate,
			long visitConceptId, 
			Long visitTypeConceptId, 
			Long visitCareSiteId, 
			Long visitProviderId, 
			String visitSourceValue) {
		Row visitOccurrence = new Row();
		long retId = visitOccurrenceId;
		visitOccurrence.add("visit_occurrence_id", visitOccurrenceId++);
		visitOccurrence.add("person_id", persId);
		visitOccurrence.add("visit_concept_id", visitConceptId); 
		visitOccurrence.add("visit_start_date", visitStartDate != null ? visitStartDate : "");
		visitOccurrence.add("visit_start_datetime", "");
		visitOccurrence.add("visit_end_date", visitEndDate != null ? visitEndDate : "");
		visitOccurrence.add("visit_end_datetime", "");
		visitOccurrence.add("visit_type_concept_id", (visitTypeConceptId != null ? visitTypeConceptId.toString() : "")); 
		visitOccurrence.add("provider_id", (visitProviderId != null ? visitProviderId.toString() : ""));
		visitOccurrence.add("care_site_id", (visitCareSiteId != null ? visitCareSiteId.toString() : ""));
		visitOccurrence.add("visit_source_value", visitSourceValue != null ? visitSourceValue : "");
		visitOccurrence.add("visit_source_concept_id", "");
		tableToRows.put("visit_occurrence", visitOccurrence);
		return retId;
	}

	//********************************************************************************//
	private String createSourceValue(String sourceTable, String sourceField, String sourceValue) {
		String returnValue = sourceValue != null ? sourceValue : "";
		if (USE_VERBOSE_SOURCE_VALUES) {
			if (sourceTable.length() > 0) {
				sourceField = sourceField != null ? sourceField : "";
				sourceValue = sourceValue != null ? sourceValue : "";
				returnValue = sourceTable + ";" + sourceField + ";" + sourceValue;
			}
		}
		return returnValue;
	}

	//********************************************************************************//
	private String createProviderRef(String sourceTable, String sourceValue) {
		String returnValue = sourceValue != null ? sourceValue : "";
		if (sourceTable.length() > 0) {
			sourceValue = sourceValue != null ? sourceValue : "";
			returnValue = sourceTable + sourceValue;
		}
		return returnValue;
	}

	//********************************************************************************//
	private String createVisitRef(String lopnr, String diagnodatum) {
		String returnValue = null;
		if (lopnr.length() > 0) {
			diagnodatum = diagnodatum != null ? diagnodatum : "";
			returnValue = lopnr + "_" + diagnodatum;
		}
		return returnValue;
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
		connection.executeResource(sqlFile);
	}

	//********************  ********************//
	private String formatDateFrom8To10(String inDate) {
		String retDate = inDate;
		if ((inDate != null) && (inDate.length() == 8))
			retDate = inDate.substring(0, 4) + "-" + inDate.substring(4, 6) + "-" + inDate.substring(6,  8);
		return(retDate);
	}

	//********************  ********************//
	private String removeLeadingZeroes(String string) {
		for (int i = 0; i < string.length(); i++)
			if (string.charAt(i) != '0')
				return string.substring(i);
		return string;
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
				String nonAllowedNullField = cdmv4NullableChecker.findNonAllowedNull(table, row);
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
	private CodeDomainData getCauseOfDeathConcept(String causeOfDeathCode, String icdVersion) {
		String altCode = null;
		CodeDomainData causeData = null;
		switch (icdVersion) {
		case "10":
			causeData = icd10ToConcept.getCodeData(causeOfDeathCode);
			break;
		case "9":
			causeData = icd9ToConcept.getCodeData(causeOfDeathCode);
			break;
		default:
			break;
		}
		return causeData;
	}

	//********************  ********************//
	private String getField(String srcFile, String key) {
		String fieldName;
		try {
			switch (srcFile) {
			case "patient":
				fieldName = patientKeyToFieldName.get(key.toLowerCase());
				break;				
			case "cancer":
				fieldName = cancerKeyToFieldName.get(key.toLowerCase());
				break;				
			case "death":
				fieldName = deathKeyToFieldName.get(key.toLowerCase());
				break;				
			case "drug":
				fieldName = drugKeyToFieldName.get(key.toLowerCase());
				break;
			default:
				fieldName = null;
				break;	
			}
		} catch (Exception e) {
			fieldName = null;
		}
		return fieldName;
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
