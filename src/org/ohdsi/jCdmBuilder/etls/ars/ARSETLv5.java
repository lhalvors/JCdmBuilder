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
package org.ohdsi.jCdmBuilder.etls.ars.v5;

import java.io.File;
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
import org.ohdsi.utilities.files.Row;

public class ARSETLv5 {

	public static String				EXTRACTION_DATE_DEFAULT			= "2015-12-31"; //"2012-12-31";
	public static String				MIN_OBSERVATION_DATE_DEFAULT	= "2003-01-01";
	public static String				DATE_PLACEHOLDER_DEFAULT		= "9999-12-31";
	public static int					MAX_ROWS_PER_PERSON_DEFAULT		= 100000;
	public static long					BATCH_SIZE_DEFAULT				= 1000;
	public static boolean				GENERATE_QASAMPLES_DEFAULT		= false;
	public static double				QA_SAMPLE_PROBABILITY_DEFAULT	= 0.001;
	public static String[]				TABLES							= new String[] { "DDRUG", "DRUGS", "EXE", "HOSP", "OUTPAT", "PERSON", "DEATH" };
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
	private long						minObservationDate;

	private OneToManyList<String, Row>	tableToRows;
	private CodeToDomainConceptMap		atcToConcept;
	private Map<String, String>			procToType;
	private CodeToDomainConceptMap		icd9ToConcept;
	private CodeToDomainConceptMap		icd9ProcToConcept;
	private CodeToConceptMap			specialtyToConcept;
	private Map<String, Long>			hospRowToVisitOccurrenceId		= new HashMap<String, Long>();
	private Map<String, Long>			outpatRowToVisitOccurrenceId	= new HashMap<String, Long>();
	private Map<String, Long>			sourceToProviderId				= new HashMap<String, Long>();
	private Map<Long, String>			patientToDeathDate				= new HashMap<Long, String>();
	private QCSampleConstructor			qcSampleConstructor;
	private EtlReport					etlReport;
	private CdmV5NullableChecker		cdmv4NullableChecker			= new CdmV5NullableChecker();

	private IniFile						settings;
	private long						batchSize;				// How many patient records, and all related records, should be loaded before inserting batch in target
	public long							maxRowsPerPerson;		// Maximum rows of data in source tables per person
	public String						extractionDateStr;		// Extraction date
	public String						minObservationDateStr;	// Minimum observation date
	public String						datePlaceHolder;		// Place holder value for dates - this value will be considered valid, but will not be recorded in the target date field
	public Boolean						generateQaSamples;		// Generate QA Samples at end of ETL
	public double						qaSampleProbability;	// The sample probability value used to include a patient's records in the QA Sample export
	
	// ******************** Process ********************//
	public void process(String folder, DbSettings dbSettings, int maxPersons, int versionId) {
		this.folder = folder;
		loadSettings();
		extractionDate = StringUtilities.databaseTimeStringToDays(extractionDateStr);
		minObservationDate = StringUtilities.databaseTimeStringToDays(minObservationDateStr);
		System.out.println("Extraction date is set at " + extractionDateStr);
		System.out.println("Minimum observation date is set at " + minObservationDateStr);

		checkTablesForFormattingErrors();
		sortTables();
		connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		connection.setContext(this.getClass());
		connection.use(dbSettings.database);
		loadMappings(dbSettings);
		List<String> tableNames = connection.getTableNames(dbSettings.database);

		if (!tableNames.contains("cost"))
			createCostTable(dbSettings);
		if (!tableNames.contains("_version"))
			connection.execute("CREATE TABLE _version(version_id INT,version_date DATE);");	
		
		truncateTables(connection);

		String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
		connection.execute("INSERT INTO _version (version_id, version_date) VALUES (" + versionId + ", '" + date + "')");

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
		patientToDeathDate.clear();
		if (generateQaSamples)
			qcSampleConstructor = new QCSampleConstructor(folder + "/sample", qaSampleProbability);//0.0001);
		tableToRows = new OneToManyList<String, Row>();
		etlReport = new EtlReport(folder);

		StringUtilities.outputWithTime("Populating CDM_Source table");
		populateCdmSourceTable();

		StringUtilities.outputWithTime("Processing persons");
		MultiRowIterator iterator = constructMultiRowIterator();
		while (iterator.hasNext()) {
			processPerson(iterator.next());

			if (personCount == maxPersons) {
				System.out.println("Reached limit of " + maxPersons + " persons, terminating");
				break;
			}
			if (personCount % batchSize == 0) {
				insertBatch();
				System.out.println("Processed " + personCount + " persons");
			}
		}
		insertBatch();
		System.out.println("Processed " + personCount + " persons");
		if (generateQaSamples)
			qcSampleConstructor.addCdmData(connection, dbSettings.database);

		String etlReportName = etlReport.generateETLReport(icd9ProcToConcept, icd9ToConcept, atcToConcept);
		System.out.println("An ETL report was generated and written to :" + etlReportName);

		String etlReportName2 = etlReport.generateETLReport(specialtyToConcept);
		System.out.println("An ETL report was generated and written to :" + etlReportName2);

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
			FileSorter.sort(folder + "/" + table + ".csv", "PERSON_ID");
		}
		StringUtilities.outputWithTime("Finished sorting");
	}

	//********************  ********************//
	private void insertBatch() {
		removeRowsWithNonNullableNulls();
		removeRowsOutsideOfObservationTime();

		etlReport.registerOutgoingData(tableToRows);
		for (String table : tableToRows.keySet())
			connection.insertIntoTable(tableToRows.get(table).iterator(), table, false, true);
		tableToRows.clear();
	}

	//********************  ********************//
	private void loadMappings(DbSettings dbSettings) {
		StringUtilities.outputWithTime("Loading mappings from server");
		RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		connection.setContext(this.getClass());
		connection.use(dbSettings.database);

		System.out.println("- Loading ATC to concept_id mapping");
		atcToConcept = new CodeToDomainConceptMap("ATC to concept_id mapping", "Drug");
		for (Row row : connection.queryResource("sql/atcToRxNorm.sql")) {
			row.upperCaseFieldNames();
			atcToConcept.add(row.get("SOURCE_CODE"), row.get("SOURCE_NAME"), row.getInt("SOURCE_CONCEPT_ID"), row.getInt("TARGET_CONCEPT_ID"),
					row.get("TARGET_CODE"), row.get("TARGET_NAME"), row.get("DOMAIN_ID"));
		}

		System.out.println("- Loading ICD-9 to concept_id mapping");
		icd9ToConcept = new CodeToDomainConceptMap("ICD-9 to concept_id mapping", "Condition");
		for (Row row : connection.queryResource("sql/icd9ToConditionProcMeasObsDevice.sql")) {
			row.upperCaseFieldNames();
			icd9ToConcept.add(row.get("SOURCE_CODE"), row.get("SOURCE_NAME"), row.getInt("SOURCE_CONCEPT_ID"), row.getInt("TARGET_CONCEPT_ID"),
					row.get("TARGET_CODE"), row.get("TARGET_NAME"), row.get("DOMAIN_ID"));
		}

		System.out.println("- Loading ICD-9 Procedure to concept_id mapping");
		icd9ProcToConcept = new CodeToDomainConceptMap("ICD-9 Procedure to concept_id mapping", "Procedure");
		for (Row row : connection.queryResource("sql/icd9ProcToProcMeasObsDrugCondition.sql")) {
			row.upperCaseFieldNames();
			icd9ProcToConcept.add(row.get("SOURCE_CODE"), row.get("SOURCE_NAME"), row.getInt("SOURCE_CONCEPT_ID"), row.getInt("TARGET_CONCEPT_ID"),
					row.get("TARGET_CODE"), row.get("TARGET_NAME"), row.get("DOMAIN_ID"));
		}

		System.out.println("- Loading specialty to concept mapping");
		specialtyToConcept = new CodeToConceptMap("Specialty to concept mapping");
		for (Row row : new ReadCSVFileWithHeader(this.getClass().getResourceAsStream("csv/specialty_italian.csv"))) {
			row.upperCaseFieldNames();
			specialtyToConcept.add(removeLeadingZeroes(row.get("COD")), row.get("DESCRIZIONE"), row.getInt("CONCEPT_ID"), row.get("CONCEPT_CODE"),
					row.get("CONCEPT_NAME"));
		}

		System.out.println("- Loading proc_cod to type_outpat mapping");
		procToType = new HashMap<String, String>();
		for (Row row : new ReadCSVFileWithHeader(this.getClass().getResourceAsStream("csv/proc_OUTPAT.csv"))) {
			row.upperCaseFieldNames();
			procToType.put(row.get("PROC_COD"), row.get("TYPE_OUTPAT"));
		}
		StringUtilities.outputWithTime("Finished loading mappings");
	}

	//********************  ********************//
	private void loadSettings() {
		batchSize = BATCH_SIZE_DEFAULT;
		maxRowsPerPerson = MAX_ROWS_PER_PERSON_DEFAULT;
		extractionDateStr = EXTRACTION_DATE_DEFAULT;
		minObservationDateStr = MIN_OBSERVATION_DATE_DEFAULT;
		datePlaceHolder = DATE_PLACEHOLDER_DEFAULT;
		generateQaSamples = GENERATE_QASAMPLES_DEFAULT;
		qaSampleProbability = QA_SAMPLE_PROBABILITY_DEFAULT;
		
		File f = new File("ARS.ini");
		if(f.exists() && !f.isDirectory()) { 
			try {
				settings = new IniFile("ARS.ini");
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
					String strParam = settings.get("extractionDate");
					if ((strParam != null) && (CSVFileChecker.isDateFormat1(strParam)))
						extractionDateStr = strParam;
					else
						extractionDateStr = EXTRACTION_DATE_DEFAULT;
				} catch (Exception e) {
					extractionDateStr = EXTRACTION_DATE_DEFAULT;
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
					String strParam = settings.get("datePlaceHolder");
					if ((strParam != null) && (strParam.length() == 10))
						datePlaceHolder = strParam;
					else
						datePlaceHolder = DATE_PLACEHOLDER_DEFAULT;
				} catch (Exception e) {
					datePlaceHolder = DATE_PLACEHOLDER_DEFAULT;
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
		return new MultiRowIterator("PERSON_ID", TABLES, iterators);
	}

	//********************  ********************//
	private void fixDateProblem(MultiRowSet personData) {
		for (List<Row> rows : personData.values())
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

	//******************** processPerson ********************//
	private void processPerson(MultiRowSet personData) {
		etlReport.registerIncomingData(personData);

		if (personData.linkingId.trim().length() == 0) {
			for (String table : personData.keySet())
				if (personData.get(table).size() > 0)
					etlReport.reportProblem(table, "Missing person_id in " + personData.get(table).size() + " rows in table " + table, "");
			return;
		}

		if (personData.totalSize() > maxRowsPerPerson) {
			etlReport.reportProblem("", "Person has too many rows (" + personData.totalSize() + "). Skipping person", personData.linkingId);
			return;
		}

		personCount++;
		personId++;

		if (generateQaSamples)
			qcSampleConstructor.registerPersonData(personData, personId);
		fixDateProblem(personData);

		hospRowToVisitOccurrenceId.clear();
		outpatRowToVisitOccurrenceId.clear();
		addToProvider(personData);
		addToDeath(personData);
		addToPerson(personData);
		addToObservationPeriod(personData);
		addToVisitOccurrence(personData);
		addToDrugExposure(personData);
		addToConditionOccurrence(personData);
		addToProcedureOccurrence(personData);
	}

	//******************** PROVIDER ********************//
	private void addToProvider(MultiRowSet personData) {
		for (Row row : personData.get("OUTPAT")) {
			if (row.get("GROUP_CODE").length() > 0) {
				String groupCode = row.get("GROUP_CODE");
				String sourceCodeVal = createSourceValue("OUTPAT","GROUP_CODE",groupCode);
				String providerRef = createProviderRef("OUTPAT",groupCode);
				if (sourceToProviderId.get(providerRef) == null) { // Did not encounter this group code before
					int specialtyConceptId = specialtyToConcept.getConceptId(removeLeadingZeroes(groupCode));

					Row provider = new Row();
					sourceToProviderId.put(providerRef, providerId);
					provider.add("provider_id", providerId++);
//					provider.add("provider_name", providerRef);
					provider.add("specialty_concept_id", specialtyConceptId);
					provider.add("specialty_source_value", groupCode);
					provider.add("provider_source_value", sourceCodeVal);
					tableToRows.put("provider", provider);
				}
			}
		}

		for (Row row : personData.get("HOSP")) {
			if (row.get("WARD_DISCHARGE").length() > 0) {
				String wardDischarge = row.get("WARD_DISCHARGE");
				String sourceCodeVal = createSourceValue("HOSP","WARD_DISCHARGE",wardDischarge);
				String providerRef = createProviderRef("HOSP",wardDischarge);
				if (sourceToProviderId.get(providerRef) == null) { // Did not encounter this specialty code before
					String specialtyCode = "";
					int specialtyConceptId = 0;
					if (wardDischarge.length() > 1) {
						specialtyCode = wardDischarge.substring(0, 2);
						specialtyConceptId = specialtyToConcept.getConceptId(removeLeadingZeroes(specialtyCode));
					}

					Row provider = new Row();
					sourceToProviderId.put(providerRef, providerId);
					provider.add("provider_id", providerId++);
//					provider.add("provider_name", providerRef);
					provider.add("specialty_concept_id", specialtyConceptId);
					provider.add("specialty_source_value", specialtyCode);
					provider.add("provider_source_value", sourceCodeVal);
					tableToRows.put("provider", provider);
				}
			}
		}

		for (Row row : personData.get("PERSON")) {
			if (row.get("GP_ID").length() != 0) {
				String gpId = row.get("GP_ID");
				String sourceCodeVal = createSourceValue("PERSON","GP_ID",gpId);
				String providerRef = createProviderRef("PERSON",gpId);
				if (sourceToProviderId.get(providerRef) == null) { // Did not encounter this gp_id before
					Row provider = new Row();
					sourceToProviderId.put(providerRef, providerId);
					provider.add("provider_id", providerId++);
//					provider.add("provider_name", providerRef);
					provider.add("specialty_concept_id", 38004446); // General Practice
					provider.add("specialty_source_value", "");
					provider.add("provider_source_value", sourceCodeVal);
					tableToRows.put("provider", provider);
				}
			}
		}
	}

	//******************** PROCEDURE OCCURRENCE - OBSERVATION ********************//
	private void addToProcedureOccurrence(MultiRowSet personData) {
		for (Row row : personData.get("HOSP")) {
			String startDate = checkDateString(row.get("START_DATE"));
			String mainProcDate = checkDateString(row.get("DATE_MAIN_PROC"));
			Long hospProviderId = sourceToProviderId.get(createProviderRef("HOSP",row.get("WARD_DISCHARGE")));
			if (mainProcDate.length() == 0)
				mainProcDate = startDate;
			Long visitId = hospRowToVisitOccurrenceId.get(row.toString());
			addHospProcedure(row.get("MAIN_PROC"), "MAIN_PROC", row.get("PERSON_ID"), mainProcDate, visitId, hospProviderId, 38000248);
			addHospProcedure(row.get("SECONDARY_PROC_1"), "SECONDARY_PROC_1", row.get("PERSON_ID"), startDate, visitId, hospProviderId, 38000249);
			addHospProcedure(row.get("SECONDARY_PROC_2"), "SECONDARY_PROC_2", row.get("PERSON_ID"), startDate, visitId, hospProviderId, 38000249);
			addHospProcedure(row.get("SECONDARY_PROC_3"), "SECONDARY_PROC_3", row.get("PERSON_ID"), startDate, visitId, hospProviderId, 38000249);
			addHospProcedure(row.get("SECONDARY_PROC_4"), "SECONDARY_PROC_4", row.get("PERSON_ID"), startDate, visitId, hospProviderId, 38000249);
			addHospProcedure(row.get("SECONDARY_PROC_5"), "SECONDARY_PROC_5", row.get("PERSON_ID"), startDate, visitId, hospProviderId, 38000249);
		}

		for (Row row : personData.get("OUTPAT")) {
			String procDate = checkDateString(row.get("PROC_START_DATE"));
			if (procDate.length() == 0)
				procDate = checkDateString(row.get("PROC_DATE"));
			Long opProviderId = sourceToProviderId.get(createProviderRef("OUTPAT",row.get("GROUP_CODE")));
			if (procDate.length() != 0) { // Currently suppressing problems due to missing dates
				Long visitId = outpatRowToVisitOccurrenceId.get(row.toString());
				addHospProcedure(row.get("GROUP_CODE"), "GROUP_CODE", row.get("PERSON_ID"), procDate, visitId, opProviderId, 38000266);
			}
		}
	}

	private void addHospProcedure(String proc, String procKey, String sourceKeyVal, String date, Long visitId, Long hpProviderId, int procedureTypeConceptId) {
		proc = proc.trim();
		if (proc.length() != 0) {
			String providerId = "";
			if (hpProviderId != null)
				providerId = Long.toString(hpProviderId);
			CodeDomainData data = icd9ProcToConcept.getCodeData(proc);
			for (TargetConcept targetConcept : data.targetConcepts) {
				String targetDomain = targetConcept.domainId;
				switch (targetDomain) {
				case "Observation":
					Row observation = new Row();
					observation.add("observation_id", observationId++);
					observation.add("person_id", personId);
					observation.add("observation_concept_id", targetConcept.conceptId);				
					observation.add("observation_date", date);
					observation.add("observation_type_concept_id", 0);				
					observation.add("visit_occurrence_id", (visitId == null ? "" : visitId.toString()));
					if (procKey.equals("GROUP_CODE"))
						observation.add("observation_source_value", createSourceValue("OUTPAT",procKey,proc));
					else
						observation.add("observation_source_value", createSourceValue("HOSP",procKey,proc));
					observation.add("provider_id", providerId);
					tableToRows.put("observation", observation);
					break;
				case "Procedure":
					Row procedureOccurrence = new Row();
					procedureOccurrence.add("procedure_occurrence_id", procedureOccurrenceId++);
					procedureOccurrence.add("person_id", personId);
					procedureOccurrence.add("procedure_concept_id", targetConcept.conceptId);
					procedureOccurrence.add("procedure_date", date);
					procedureOccurrence.add("procedure_type_concept_id", procedureTypeConceptId);
					procedureOccurrence.add("visit_occurrence_id", (visitId == null ? "" : visitId.toString()));
					if (procKey.equals("GROUP_CODE"))
						procedureOccurrence.add("procedure_source_value", createSourceValue("OUTPAT",procKey,proc));
					else
						procedureOccurrence.add("procedure_source_value", createSourceValue("HOSP",procKey,proc));
					procedureOccurrence.add("procedure_source_concept_id", data.sourceConceptId);//**
					procedureOccurrence.add("provider_id", providerId);
					tableToRows.put("procedure_occurrence", procedureOccurrence);
					break;
				default:
					System.out.println("Other domain from hospProcedure:"+targetDomain+", "+proc+", "+targetConcept.conceptId+", "+personId); 
					break;
				}
			}
		}
	}

	//******************** VISIT OCCURRENCE ********************//
	private void addToVisitOccurrence(MultiRowSet personData) {
		for (Row row : personData.get("HOSP")) {
			hospRowToVisitOccurrenceId.put(row.toString(), visitOccurrenceId);

			Row visitOccurrence = new Row();
			String startDate = checkDateString(row.get("START_DATE"));
			String endDate = checkDateString(row.get("END_DATE"));
			visitOccurrence.add("visit_occurrence_id", visitOccurrenceId++);
			visitOccurrence.add("person_id", personId);
			visitOccurrence.add("visit_start_date", startDate);
			if (endDate.length() == 0)
				visitOccurrence.add("visit_end_date", startDate);
			else
				visitOccurrence.add("visit_end_date", endDate);
			visitOccurrence.add("visit_concept_id", 9201); // Inpatient visit
			visitOccurrence.add("visit_type_concept_id", 44818517); // Visit derived from encounter on claim
			visitOccurrence.add("visit_source_value", createSourceValue("HOSP",null,null));
			tableToRows.put("visit_occurrence", visitOccurrence);
		}

		for (Row row : personData.get("OUTPAT")) {
			String type = procToType.get(row.get("PROC_COD"));
			if (type != null && type.equals("CLIN")) {
				outpatRowToVisitOccurrenceId.put(row.toString(), visitOccurrenceId);

				Row visitOccurrence = new Row();
				String procStartDate = checkDateString(row.get("PROC_START_DATE"));
				String procEndDate = checkDateString(row.get("PROC_END_DATE"));
				visitOccurrence.add("visit_occurrence_id", visitOccurrenceId++);
				visitOccurrence.add("person_id", personId);
				visitOccurrence.add("visit_start_date", procStartDate);
				if (procEndDate.length() == 0)
					visitOccurrence.add("visit_end_date", procStartDate);
				else
					visitOccurrence.add("visit_end_date", procEndDate);
				visitOccurrence.add("visit_concept_id", 9202); // Outpatient visit
				visitOccurrence.add("visit_type_concept_id", 44818517); // Visit derived from encounter on claim
				visitOccurrence.add("visit_source_value", createSourceValue("OUTPAT",null,null));
				tableToRows.put("visit_occurrence", visitOccurrence);
			}
		}
	}

	//******************** CONDITION OCCURRENCE - MEASUREMENT - OBSERVATION ********************//
	private void addToConditionOccurrence(MultiRowSet personData) {
		for (Row row : personData.get("HOSP")) {
			String startDate = checkDateString(row.get("START_DATE"));
			Long visitId = hospRowToVisitOccurrenceId.get(row.toString());
			Long coProviderId = sourceToProviderId.get(createProviderRef("HOSP",row.get("WARD_DISCHARGE")));
			addHospCondition(startDate, row.get("MAIN_DIAGNOSIS"), "MAIN_DIAGNOSIS", row.get("PERSON_ID"), visitId, coProviderId, 38000183); // Inpatient detail, primary
			addHospCondition(startDate, row.get("SECONDARY_DIAGNOSIS_1"), "SECONDARY_DIAGNOSIS_1", row.get("PERSON_ID"), visitId, coProviderId, 38000184); // Inpatient detail - 1st position
			addHospCondition(startDate, row.get("SECONDARY_DIAGNOSIS_2"), "SECONDARY_DIAGNOSIS_2", row.get("PERSON_ID"), visitId, coProviderId, 38000185); // Inpatient detail - 2nd position
			addHospCondition(startDate, row.get("SECONDARY_DIAGNOSIS_3"), "SECONDARY_DIAGNOSIS_3", row.get("PERSON_ID"), visitId, coProviderId, 38000186); // Inpatient detail - 3rd position
			addHospCondition(startDate, row.get("SECONDARY_DIAGNOSIS_4"), "SECONDARY_DIAGNOSIS_4", row.get("PERSON_ID"), visitId, coProviderId, 38000187); // Inpatient detail - 4th position
			addHospCondition(startDate, row.get("SECONDARY_DIAGNOSIS_5"), "SECONDARY_DIAGNOSIS_5", row.get("PERSON_ID"), visitId, coProviderId, 38000188); // Inpatient detail - 5th position
		}

		for (Row row : personData.get("EXE")) {
			CodeDomainData data = icd9ToConcept.getCodeData(row.get("EXEMPTION_CODE"));
			for (TargetConcept targetConcept : data.targetConcepts) {
				Row conditionOccurrence = new Row();
				conditionOccurrence.add("condition_occurrence_id", conditionOccurrenceId++);
				conditionOccurrence.add("person_id", personId);
				conditionOccurrence.add("condition_concept_id", targetConcept.conceptId);
				conditionOccurrence.add("condition_start_date", checkDateString(row.get("EXE_START_DATE")));
				conditionOccurrence.add("condition_type_concept_id", 38000245);  // (EHR problem list entry) Need better code for exe
				conditionOccurrence.add("visit_occurrence_id", "");
				conditionOccurrence.add("condition_source_concept_id", data.sourceConceptId);
				conditionOccurrence.add("condition_source_value", createSourceValue("EXE","EXEMPTION_CODE",row.get("EXEMPTION_CODE")));
				conditionOccurrence.add("provider_id", "");
				tableToRows.put("condition_occurrence", conditionOccurrence);
			}
		}
	}

	private void addHospCondition(String startDate, String diagnose, String condKey, String sourceKeyVal, Long visitId, Long hcProviderId, int typeConceptId) {
		diagnose = diagnose.trim();
		if (diagnose.length() != 0) {
			String providerId = "";
			if (hcProviderId != null)
				providerId = Long.toString(hcProviderId);
			CodeDomainData data = icd9ToConcept.getCodeData(diagnose);
			for (TargetConcept targetConcept : data.targetConcepts) {
				String targetDomain = targetConcept.domainId;
				switch (targetDomain) {
				case "Measurement":
					Row measurement = new Row();
					measurement.add("measurement_id", measurementId++);
					measurement.add("person_id", personId);
					measurement.add("measurement_concept_id", targetConcept.conceptId);
					measurement.add("measurement_date", startDate);
					measurement.add("measurement_type_concept_id", typeConceptId);
					measurement.add("visit_occurrence_id", (visitId == null ? "" : visitId.toString()));
					measurement.add("measurement_source_concept_id", data.sourceConceptId);
					measurement.add("measurement_source_value", createSourceValue("HOSP",condKey,diagnose));
					measurement.add("provider_id", providerId);
					tableToRows.put("measurement", measurement);
					break;
				case "Observation":
					Row observation = new Row();
					observation.add("observation_id", observationId++);
					observation.add("person_id", personId);
					observation.add("observation_concept_id", targetConcept.conceptId);				
					observation.add("observation_date", startDate);
					observation.add("observation_type_concept_id", typeConceptId);				
					observation.add("visit_occurrence_id", (visitId == null ? "" : visitId.toString()));
					observation.add("observation_source_concept_id", data.sourceConceptId);
					observation.add("observation_source_value", createSourceValue("HOSP",condKey,diagnose));
					observation.add("provider_id", providerId);
					tableToRows.put("observation", observation);
					break;
				case "Procedure":
					Row procedureOccurrence = new Row();
					procedureOccurrence.add("procedure_occurrence_id", procedureOccurrenceId++);
					procedureOccurrence.add("person_id", personId);
					procedureOccurrence.add("procedure_concept_id", targetConcept.conceptId);
					procedureOccurrence.add("procedure_date", startDate);
					procedureOccurrence.add("procedure_type_concept_id", typeConceptId);
					procedureOccurrence.add("visit_occurrence_id", (visitId == null ? "" : visitId.toString()));
					procedureOccurrence.add("procedure_source_concept_id", data.sourceConceptId);
					procedureOccurrence.add("procedure_source_value", createSourceValue("HOSP",condKey,diagnose));
					procedureOccurrence.add("provider_id", providerId);
					tableToRows.put("procedure_occurrence", procedureOccurrence);
					break;
				case "Condition":
					Row conditionOccurrence = new Row();
					conditionOccurrence.add("condition_occurrence_id", conditionOccurrenceId++);
					conditionOccurrence.add("person_id", personId);
					conditionOccurrence.add("condition_concept_id", targetConcept.conceptId);
					conditionOccurrence.add("condition_start_date", startDate);
					conditionOccurrence.add("condition_type_concept_id", typeConceptId);
					conditionOccurrence.add("visit_occurrence_id", (visitId == null ? "" : visitId.toString()));
					conditionOccurrence.add("condition_source_concept_id", data.sourceConceptId);
					conditionOccurrence.add("condition_source_value", createSourceValue("HOSP",condKey,diagnose));
					conditionOccurrence.add("provider_id", providerId);
					tableToRows.put("condition_occurrence", conditionOccurrence);
					break;
				default:
					System.out.println("Other domain from hospCondition:"+targetDomain+", "+diagnose+", "+targetConcept.conceptId+", "+personId); 
					break;
				}
			}
		}
	}

	//******************** DRUG EXPOSURE ********************//
	private void addToDrugExposure(MultiRowSet personData) {
		for (Row row : personData.get("DRUGS")) {
			CodeDomainData data = atcToConcept.getCodeData(row.get("ATC"));
			for (TargetConcept targetConcept : data.targetConcepts) {
				String targetDomain = targetConcept.domainId;
				switch (targetDomain) {
				case "Drug":
					Row drugExposure = new Row();
					String daysSupply = row.get("DURATION");
					if (daysSupply.length() > 0) {
						daysSupply = Integer.toString((int) Math.round(Double.valueOf(daysSupply)));
					}
					drugExposure.add("drug_exposure_id", drugExposureId++);
					drugExposure.add("person_id", personId);
					drugExposure.add("drug_concept_id", targetConcept.conceptId);
					drugExposure.add("drug_exposure_start_date", checkDateString(row.get("DRUG_DISPENSING_DATE")));
					drugExposure.add("days_supply", daysSupply);
					drugExposure.add("drug_type_concept_id", 38000175);  // Prescription dispensed in pharmacy
					drugExposure.add("drug_source_value", createSourceValue("DRUGS","ATC",row.get("ATC")));
					drugExposure.add("drug_source_concept_id", data.sourceConceptId);
					tableToRows.put("drug_exposure", drugExposure);
					break;
				default:
					System.out.println("Other domain from DRUGS:"+targetDomain+", "+row.get("ATC")+", "+targetConcept.conceptId+", "+personId); 
					break;
				}
			}
		}

		for (Row row : personData.get("DDRUG")) {
			CodeDomainData data = atcToConcept.getCodeData(row.get("ATC"));
			for (TargetConcept targetConcept : data.targetConcepts) {
				String targetDomain = targetConcept.domainId;
				switch (targetDomain) {
				case "Drug":
					Row drugExposure = new Row();
					String daysSupply = row.get("DURATION");
					if (daysSupply.length() > 0) {
						daysSupply = Integer.toString((int) Math.round(Double.valueOf(daysSupply)));
					}
					drugExposure.add("drug_exposure_id", drugExposureId++);
					drugExposure.add("person_id", personId);
					drugExposure.add("drug_concept_id", targetConcept.conceptId);
					drugExposure.add("drug_exposure_start_date", checkDateString(row.get("DRUG_DISPENSING_DATE")));
					drugExposure.add("days_supply", daysSupply);
					drugExposure.add("drug_type_concept_id", 38000175);  // Prescription dispensed in pharmacy
					drugExposure.add("drug_source_value", createSourceValue("DDRUG","ATC",row.get("ATC")));
					drugExposure.add("drug_source_concept_id", data.sourceConceptId);
					tableToRows.put("drug_exposure", drugExposure);
					break;
				default:
					System.out.println("Other domain from DDRUG:"+targetDomain+", "+row.get("ATC")+", "+targetConcept.conceptId+", "+personId); 
					break;
				}
			}
		}
	}

	//******************** OBSERVATION PERIOD ********************//

	private void addToObservationPeriod(MultiRowSet personData) {
		List<Row> rows = personData.get("PERSON");
		if (rows.size() == 0)
			return;

		RowUtilities.sort(rows, "STARTDATE");
		long observationPeriodStartDate = Long.MIN_VALUE;
		long observationPeriodEndDate = Long.MIN_VALUE;
		for (Row row : rows) {
			if (checkDateString(row.get("STARTDATE")).length() == 0) {
				etlReport.reportProblem("PERSON", "No person startdate, could not create observation period", row.get("PERSON_ID"));
				continue;
			}
			long startDate = StringUtilities.databaseTimeStringToDays(checkDateString(row.get("STARTDATE")));
			startDate = Math.max(startDate, minObservationDate);
			long endDate = StringUtilities.databaseTimeStringToDays(checkDateString(row.get("ENDDATE")));
			endDate = endDate == StringUtilities.MISSING_DATE ? extractionDate : Math.min(endDate, extractionDate);
			if (observationPeriodEndDate == startDate - 1) {
				observationPeriodEndDate = endDate;
			} else {
				if (observationPeriodStartDate != Long.MIN_VALUE && observationPeriodStartDate < observationPeriodEndDate) {
					Row observationPeriod = new Row();
					observationPeriod.add("observation_period_id", observationPeriodId++);
					observationPeriod.add("person_id", personId);
					observationPeriod.add("observation_period_start_date", StringUtilities.daysToDatabaseDateString(observationPeriodStartDate));
					observationPeriod.add("observation_period_end_date", StringUtilities.daysToDatabaseDateString(observationPeriodEndDate));
					observationPeriod.add("period_type_concept_id", "");
					tableToRows.put("observation_period", observationPeriod);
				}
				observationPeriodStartDate = startDate;
				observationPeriodEndDate = endDate;	
			}
		}
		if (observationPeriodStartDate != Long.MIN_VALUE && observationPeriodStartDate < observationPeriodEndDate) {
			Row observationPeriod = new Row();
			observationPeriod.add("observation_period_id", observationPeriodId++);
			observationPeriod.add("person_id", personId);
			observationPeriod.add("observation_period_start_date", StringUtilities.daysToDatabaseDateString(observationPeriodStartDate));
			observationPeriod.add("observation_period_end_date", StringUtilities.daysToDatabaseDateString(observationPeriodEndDate));
			observationPeriod.add("period_type_concept_id", 44814724);  // Period covering healthcare encounters
			tableToRows.put("observation_period", observationPeriod);
		}
	}

	//******************** DEATH ********************//

	private void addToDeath(MultiRowSet personData) {
		for (Row row : personData.get("DEATH")) {
			String dateOfDeath = checkDateString(row.get("DATE_OF_DEATH"));
			if (dateOfDeath.trim().length() != 0) {
				String altCode = null;
				String causeOfDeathCode = row.get("CAUSE_OF_DEATH");
				if (causeOfDeathCode.length() == 4 && causeOfDeathCode.charAt(3) == '0') {
					altCode = causeOfDeathCode.substring(0, 3);
				}
				CodeDomainData causeData = icd9ToConcept.getCodeData(causeOfDeathCode);
				if ((causeData.targetConcepts.get(0).conceptId == 0) && (altCode != null)) {
					causeData = icd9ToConcept.getCodeData(altCode);
				}
				
				patientToDeathDate.put(personId, dateOfDeath);
				Row death = new Row();
				death.add("person_id", personId);
				death.add("death_type_concept_id", 38003565);  // Payer enrollment status "Deceased"
				death.add("death_date", dateOfDeath);
				death.add("cause_concept_id", causeData.targetConcepts.get(0).conceptId); 
				death.add("cause_source_value", createSourceValue("DEATH","CAUSE_OF_DEATH",causeOfDeathCode));
				death.add("cause_source_concept_id", causeData.sourceConceptId);
				tableToRows.put("death", death);
				
				// Record as CONDITION_OCCURRENCE as well
				for (TargetConcept targetConcept : causeData.targetConcepts) {
					String targetDomain = targetConcept.domainId;
					switch (targetDomain) {
					case "Condition":
						Row conditionOccurrence = new Row();
						conditionOccurrence.add("condition_occurrence_id", conditionOccurrenceId++);
						conditionOccurrence.add("person_id", personId);
						conditionOccurrence.add("condition_concept_id", targetConcept.conceptId);
						conditionOccurrence.add("condition_start_date", dateOfDeath);
						conditionOccurrence.add("condition_type_concept_id", "44786627"); // Primary Condition
						conditionOccurrence.add("visit_occurrence_id", "");
						conditionOccurrence.add("condition_source_concept_id", causeData.sourceConceptId);
						conditionOccurrence.add("condition_source_value", createSourceValue("DEATH","CAUSE_OF_DEATH",causeOfDeathCode));
						conditionOccurrence.add("provider_id", providerId);
						tableToRows.put("condition_occurrence", conditionOccurrence);
						break;
					default:
						System.out.println("Other domain from death:"+targetDomain+", "+causeOfDeathCode+", "+targetConcept.conceptId+", "+personId); 
						break;
					}
				}
			}
		}
	}

	//******************** PERSON ********************//

	private void addToPerson(MultiRowSet personData) {
		List<Row> rows = personData.get("PERSON");
		if (rows.size() == 0)
			return;

		Row row = rows.get(0);
		if (rows.size() > 1) { // Multiple rows: find the one with the latest startdate:
			RowUtilities.sort(rows, "STARTDATE");
			row = rows.get(rows.size() - 1);
		}

		Row person = new Row();
		person.add("person_id", personId);
		person.add("person_source_value", createSourceValue("PERSON","PERSON_ID",row.get("PERSON_ID")));
		String gender = row.get("GENDER_CONCEPT_ID");
		person.add("gender_source_value", gender);
		person.add("gender_concept_id", gender.toLowerCase().equals("m") || gender.toLowerCase().equals("1") ? "8507" : gender.toLowerCase().equals("f")
				|| gender.toLowerCase().equals("2") ? "8532" : "8521");
		String dateOfBirth = checkDateString(row.get("DATE_OF_BIRTH"));

		if (dateOfBirth.length() < 10) {
			person.add("year_of_birth", "");
			person.add("month_of_birth", "");
			person.add("day_of_birth", "");
		} else {
			person.add("year_of_birth", dateOfBirth.substring(0, 4));
			person.add("month_of_birth", dateOfBirth.substring(5, 7));
			person.add("day_of_birth", dateOfBirth.substring(8, 10));
		}
		if (row.get("GP_ID").length() != 0) {
			String sourceProvVal = createProviderRef("PERSON",row.get("GP_ID"));
			Long pProviderId = sourceToProviderId.get(sourceProvVal);
			if (pProviderId != null)
				person.add("provider_id", pProviderId);
			else
				person.add("provider_id", "");
		}
		else
			person.add("provider_id", "");
		person.add("location_id", row.get("LOCATION_CONCEPT_ID"));
		person.add("ethnicity_concept_id", 8552); // Unknown
		person.add("race_concept_id", 8552); // Unknown
		tableToRows.put("person", person);
		
		if (patientToDeathDate.get(personId) == null) { // Did not record death date for this person yet
			String dateOfDeath = checkDateString(row.get("DATE_OF_DEATH"));
			if (dateOfDeath.trim().length() != 0) {
				patientToDeathDate.put(personId, dateOfDeath);
				Row death = new Row();
				death.add("person_id", personId);
				death.add("death_type_concept_id", 38003565);  // Payer enrollment status "Deceased"
				death.add("death_date", dateOfDeath);
				death.add("cause_concept_id", ""); 
				death.add("cause_source_value", "");
				death.add("cause_source_concept_id", "");
				tableToRows.put("death", death);
			}
		}

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
	private String checkDateString(String dateStr) {
		String retStr = dateStr;
		if (dateStr.equals(datePlaceHolder))
			retStr = "";
		return retStr;
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

}
