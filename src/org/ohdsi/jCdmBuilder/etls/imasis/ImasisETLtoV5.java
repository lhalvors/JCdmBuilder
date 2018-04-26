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
package org.ohdsi.jCdmBuilder.etls.imasis;

import java.io.File;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.ohdsi.databases.RichConnection;
import org.ohdsi.jCdmBuilder.DbSettings;
import org.ohdsi.jCdmBuilder.EtlReport;
import org.ohdsi.jCdmBuilder.cdm.CdmV5NullableChecker;
import org.ohdsi.jCdmBuilder.utilities.CodeToDomainConceptMap;
import org.ohdsi.jCdmBuilder.utilities.QCSampleConstructor;
import org.ohdsi.jCdmBuilder.utilities.CodeToDomainConceptMap.CodeDomainData;
import org.ohdsi.jCdmBuilder.utilities.CodeToDomainConceptMap.TargetConcept;
import org.ohdsi.jCdmBuilder.etls.imasis.SourceToTargetTrack;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.collections.OneToManyList;
import org.ohdsi.utilities.files.ReadCSVFileWithHeader;
import org.ohdsi.utilities.files.Row;
import org.ohdsi.utilities.files.WriteTextFile;
import org.ohdsi.utilities.files.IniFile;

public class ImasisETLtoV5 {

	public static String				GPID_SOURCE_FIELD				= "id";

	public static int					MAX_ROWS_PER_PERSON				= 100000;
	public static String[]				TABLES							= new String[] { "cima_ids","diagnosis","drug",
			"drug_administration","geographical_name","icd9cm","lab_measure_unit","lab_test_name","lab_map","lab_result_name",
			"lab_report_description","lab_section_description","laboratory","patient","patient_measures","procedures","services","visit",
			"visit_annotations","visit_period","visit_readmissions","visit_type" };
	public static long					BATCH_SIZE_DEFAULT				= 1000;
	private static long					MINIMUM_FREE_MEMORY_DEFAULT		= 10000000;
	public static boolean				GENERATE_QASAMPLES_DEFAULT		= false;
	public static double				QA_SAMPLE_PROBABILITY_DEFAULT	= 0.0001;
	public static String[]				tablesInsideOP					= new String[] { "condition_occurrence", "procedure_occurrence", 
			"drug_exposure", "death", "visit_occurrence", "observation", "measurement"					};
	public static String[]				fieldInTables					= new String[] { "condition_start_date", "procedure_date", 
			"drug_exposure_start_date", "death_date", "visit_start_date", "observation_date", "measurement_date" };

	private String						folder;
	private RichConnection				sourceConnection;
	private RichConnection				targetConnection;
	private long						personCount;
	private long						personId;
	private long						conditionOccurrenceId;
	private long						visitOccurrenceId;
	private long						procedureOccurrenceId;
	private long						deviceExposureId;
	private long						locationId;
	private long						careSiteId;
	private long						observationId;
	private long						measurementId;
	private SourceToTargetTrack			s2t;

	private OneToManyList<String, Row>	tableToRows;
	private CodeToDomainConceptMap		icd9ToConcept;
	private CodeToDomainConceptMap		icd9ProcToConcept;
	private CodeToDomainConceptMap		loincToConcept;
	private CodeToDomainConceptMap		natCodeCimaIdToConcept;
	private Map<String, Long>			adminCodeToConceptId			= new HashMap<String, Long>();
	private Map<String, Long>			resultcodeToConceptId			= new HashMap<String, Long>();
	private Map<String, Long>			labmeasureunitToConceptId		= new HashMap<String, Long>();
	private Map<String, Long>			sourceToCareSiteId				= new HashMap<String, Long>();
	private Map<String, Long>			geographicalNameToProviderId	= new HashMap<String, Long>();
	private Map<String, Long>			diagnosisCodeToConceptId		= new HashMap<String, Long>();
	private Map<String, Long>			procedureCodeToConceptId		= new HashMap<String, Long>();
	private Map<Long, Long>				serviceToProviderId				= new HashMap<Long, Long>();
	private Map<String, String>			resTestToLoincCode				= new HashMap<String, String>();
	private QCSampleConstructor			qcSampleConstructor;
	private EtlReport					etlReport;
	private CdmV5NullableChecker		cdmv5NullableChecker			= new CdmV5NullableChecker();

	private IniFile						settings;
	private long						batchSize;					// How many patient records, and all related records, should be loaded before inserting batch in target
	private long						minimumFreeMemory;			// Monitor memory usage - if free memory falls below threshold, insert batch of records already loaded
	private long						memoryLogFile; 				// 0: No, 1: every time a batch is inserted in target, 2: for every patient record
	public Boolean						generateQaSamples = false;	// Generate QA Samples at end of ETL
	public double						qaSampleProbability;		// The sample probability value used to include a patient's records in the QA Sample export

	private String						memFilename;
	private WriteTextFile 				memOut;
	private String						logFilename;
	private WriteTextFile 				logOut = null;
	private DecimalFormat 				df = new DecimalFormat();
	private DecimalFormatSymbols 		dfs = new DecimalFormatSymbols();
	private Runtime						runTime = Runtime.getRuntime();

	// ******************** Process ********************//
	public void process(String folder, DbSettings sourceDbSettings, DbSettings targetDbSettings, int maxPersons, int versionId) {
		StringUtilities.outputWithTime("OS name : "+System.getProperty("os.name"));
		StringUtilities.outputWithTime("OS arch : "+System.getProperty("os.arch"));

		personId = 0;
		personCount = 0;
		conditionOccurrenceId = 0;
		visitOccurrenceId = 0;
		procedureOccurrenceId = 0;
		deviceExposureId = 0;
		observationId = 0;
		measurementId = 0;
		locationId = 0;
		careSiteId = 0;
		adminCodeToConceptId.clear();
		resultcodeToConceptId.clear();
		labmeasureunitToConceptId.clear();
		sourceToCareSiteId.clear();
		geographicalNameToProviderId.clear();
		diagnosisCodeToConceptId.clear();
		procedureCodeToConceptId.clear();
		resTestToLoincCode.clear();

		loadSettings();

		sourceConnection = new RichConnection(sourceDbSettings.server, sourceDbSettings.domain, sourceDbSettings.user, sourceDbSettings.password,
				sourceDbSettings.dbType);
		sourceConnection.setContext(this.getClass());
		sourceConnection.use(sourceDbSettings.database);

		targetConnection = new RichConnection(targetDbSettings.server, targetDbSettings.domain, targetDbSettings.user, targetDbSettings.password,
				targetDbSettings.dbType);
		targetConnection.setContext(this.getClass());
		targetConnection.use(targetDbSettings.database);

		loadMappings();

		truncateTables(targetConnection);
		this.folder = folder;

		if (memoryLogFile > 0)
			setUpMemOutFile();

		if (generateQaSamples)
			qcSampleConstructor = new QCSampleConstructor(folder + "/sample", qaSampleProbability);
		tableToRows = new OneToManyList<String, Row>();
		etlReport = new EtlReport(folder);
		s2t = new SourceToTargetTrack(folder);

		StringUtilities.outputWithTime("Populating CDM_Source table");
		populateCdmSourceTable(getVocabularyVersion());

		StringUtilities.outputWithTime("Processing reference tables");
		populateReferenceTables();

		StringUtilities.outputWithTime("Processing persons");
		//		MultiRowIterator iterator = constructMultiRowIterator();
		long personRecordsInBatch = 0;

		for (Row row : sourceConnection.query("select * from patient where gender is not null and birth_date is not null")) {
			processPatientSourceRecord(row);
			personRecordsInBatch++;

			if (personCount == maxPersons) {
				StringUtilities.outputWithTime("Reached limit of " + maxPersons + " persons, terminating");
				break;
			}
			if ((personRecordsInBatch % batchSize == 0) | (memoryGettingLow())) {
				insertBatch();
				StringUtilities.outputWithTime("Processed " + personCount + " persons");
				personRecordsInBatch = 0;
				s2t.writeBatch();
			}
		}

		insertBatch(); // Insert (last) batch
		s2t.finalize();

		StringUtilities.outputWithTime("Processed " + personCount + " persons");

		StringUtilities.outputWithTime("A source-to-target report was generated and written to :" + s2t.getTrackFile());

		if (generateQaSamples)
			qcSampleConstructor.addCdmData(targetConnection, targetDbSettings.database);

		String etlReportName = etlReport.generateETLReport(icd9ToConcept, icd9ProcToConcept, loincToConcept); //, atcToConcept);
		StringUtilities.outputWithTime("An ETL report was generated and written to :" + etlReportName);
		if (etlReport.getTotalProblemCount() > 0) {
			String etlProblemListname = etlReport.generateProblemReport();
			if (memoryLogFile > 0)
				StringUtilities.outputWithTime("Memory log file created: "+ memFilename);
			StringUtilities.outputWithTime("An ETL problem list was generated and written to :" + etlProblemListname);
		}

		StringUtilities.outputWithTime("Finished ETL");

		if (memoryLogFile > 0)
			memOut.close();

		if (logOut != null)
			logOut.close();
	}

	//********************  ********************//
	private void setUpMemOutFile() {
		memFilename = folder + "/memoryLog.txt";
		int i = 1;
		while (new File(memFilename).exists())
			memFilename = folder + "/memoryLog" + (i++) + ".txt";
		dfs.setGroupingSeparator(',');
		df.setDecimalFormatSymbols(dfs);
		memOut = new WriteTextFile(memFilename);

		if (memoryLogFile == 2)
			memOut.writeln("Action"+((char) 9)+"Time stamp"+((char) 9)+"maxMemory (M)"+((char) 9)+"totalMemory (T)"+((char) 9)+"freeMemory (F)"+((char) 9)+"Calculated free memory (M - (T - F))"+((char) 9)+"#cart_visite"+((char) 9)+"#cart_pazpbl"+((char) 9)+"#cart_terap"+((char) 9)+"#cart_press"+((char) 9)+"#cart_descriz"+((char) 9)+"#cart_riabil");
		else
			memOut.writeln("Action"+((char) 9)+"Time stamp"+((char) 9)+"maxMemory (M)"+((char) 9)+"totalMemory (T)"+((char) 9)+"freeMemory (F)"+((char) 9)+"Calculated free memory (M - (T - F))");

		StringUtilities.outputWithTime("Memory log file created: "+ memFilename);
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
	private void updateMemOutFile(String action, String xtra) {
		if (xtra == null)
			memOut.writeln(action+((char) 9)+StringUtilities.now()+((char) 9)+df.format(runTime.maxMemory())+((char) 9)
					+df.format(runTime.totalMemory())+((char) 9)+df.format(runTime.freeMemory())+((char) 9)
					+df.format(calculatedFreeMemory()));
		else
			memOut.writeln(action+((char) 9)+StringUtilities.now()+((char) 9)+df.format(runTime.maxMemory())+((char) 9)
					+df.format(runTime.totalMemory())+((char) 9)+df.format(runTime.freeMemory())+((char) 9)
					+df.format(calculatedFreeMemory())+((char) 9)+xtra);			
	}
	//********************  ********************//
	private void updateMemOutFile(String action) {
		updateMemOutFile(action, null);
	}

	//********************  ********************//
	private long calculatedFreeMemory() {
		return Runtime.getRuntime().maxMemory() - (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
	}

	//********************  ********************//
	private static String addDaysToDate(String theDate, int days) {	 // String of format yyyy-MM-dd
		DateFormat dateFormat= new SimpleDateFormat("yyyy-MM-dd");
		Date baseDate = null;
		try {
			baseDate = dateFormat.parse(theDate);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
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
	private void populateCdmSourceTable(String vocabVersion) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		targetConnection.executeResource("sql/PopulateCdmSourceTable.sql", "@today", df.format(new Date()), "@vocab", vocabVersion);
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

		if (memoryLogFile > 0)
			updateMemOutFile("Inserted batch");
	}

	//********************  ********************//
	private void loadMappings() {
		StringUtilities.outputWithTime("Loading mappings from server");

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

		System.out.println("- Loading LAB_RESULT_NAME + LAB_TEST_NUMBER to LOINC code mapping");
		for (Row row : sourceConnection.queryResource("sql/selectLoincCodesFromLabMap.sql")) {
			row.upperCaseFieldNames();
			resTestToLoincCode.put(row.get("RES_TEST_NAME").replaceAll("\u0000", "").trim(), row.get("LOINC_ID").replaceAll("\u0000", "").trim());
		}

		System.out.println("- Loading LOINC to concept_id mapping");
		loincToConcept = new CodeToDomainConceptMap("LOINC to concept_id mapping", "Measurement");
		for (Row row : targetConnection.queryResource("sql/loincToMeasurementObservation.sql")) {
			row.upperCaseFieldNames();
			loincToConcept.add(row.get("SOURCE_CODE"), row.get("SOURCE_NAME"), row.getInt("SOURCE_CONCEPT_ID"), row.getInt("TARGET_CONCEPT_ID"),
					row.get("TARGET_CODE"), row.get("TARGET_NAME"), row.get("DOMAIN_ID"));
		}

		System.out.println("- Loading ADMINISTRATIONCODE to concept_id mapping");
		for (Row row : new ReadCSVFileWithHeader(this.getClass().getResourceAsStream("csv/administrationcode.csv"))) {
			row.upperCaseFieldNames();
			adminCodeToConceptId.put(row.get("KEY").toLowerCase(), row.getLong("CONCEPT_ID"));
		}

		System.out.println("- Loading RESULT CODE mapping");
		for (Row row : new ReadCSVFileWithHeader(this.getClass().getResourceAsStream("csv/resultcodes.csv"))) {
			row.upperCaseFieldNames();
			resultcodeToConceptId.put(row.get("KEY").toLowerCase(), row.getLong("CONCEPT_ID"));
		}

		System.out.println("- Loading LAB MEASURE UNIT code mapping");
		for (Row row : new ReadCSVFileWithHeader(this.getClass().getResourceAsStream("csv/labmeasureunit.csv"))) {
			row.upperCaseFieldNames();
			labmeasureunitToConceptId.put(row.get("KEY").toLowerCase(), row.getLong("CONCEPT_ID"));
		}

		System.out.println("- Loading DIAGNOSIS CODE code mapping");
		for (Row row : new ReadCSVFileWithHeader(this.getClass().getResourceAsStream("csv/diagnosis_type_2_concept_id.csv"))) {
			row.upperCaseFieldNames();
			diagnosisCodeToConceptId.put(row.get("DIAGNOSIS_TYPE").toLowerCase(), row.getLong("CONCEPT_ID"));
		}

		System.out.println("- Loading PROCEDURE CODE code mapping");
		for (Row row : new ReadCSVFileWithHeader(this.getClass().getResourceAsStream("csv/procedure_type_2_concept_id.csv"))) {
			row.upperCaseFieldNames();
			procedureCodeToConceptId.put(row.get("PROCEDURE_TYPE").toLowerCase(), row.getLong("CONCEPT_ID"));
		}

		System.out.println("- Loading NATIONAL_CODE_CIMA_ID code mapping");
		natCodeCimaIdToConcept = new CodeToDomainConceptMap("NATIONAL_CODE_CIMA_ID to concept_id mapping", "Drug");
		for (Row row : sourceConnection.queryResource("sql/selectDistinctCodesFromNatcodeToRxnorm.sql")) {
			row.upperCaseFieldNames();
			String natCodeStr = row.get("NATIONAL_CODE_CIMA_ID").replaceAll("\u0000", "");
			for (Row targetConcept: targetConnection.query("select concept_id, concept_code, concept_name, domain_id from concept where vocabulary_id='RxNorm' and concept_code='" + row.get("RXNORM_CODE").replaceAll("\u0000", "")+"'")) {
				targetConcept.upperCaseFieldNames();
				Long conceptId = targetConcept.getLong("CONCEPT_ID");
				String srcName = row.get("RXNORM_STR").replaceAll("\u0000", "");
				Integer srcConcept = 0; //getIntValue(natCodeStr);
				Integer trgConceptId = targetConcept.getInt("CONCEPT_ID");
				String trgConceptCode = targetConcept.get("CONCEPT_CODE");
				String trgConceptName = targetConcept.get("CONCEPT_NAME"); 
				String trgDomain = targetConcept.get("DOMAIN_ID");
				natCodeCimaIdToConcept.add(natCodeStr, srcName, srcConcept, trgConceptId, trgConceptCode, trgConceptName, trgDomain);
				break;				
			}
		}

		StringUtilities.outputWithTime("Finished loading mappings");
	}

	//********************  ********************//
	private void loadSettings() {
		batchSize = BATCH_SIZE_DEFAULT; 					// How many patient records, and all related records, should be loaded before inserting batch in target (default: 1000)
		minimumFreeMemory = MINIMUM_FREE_MEMORY_DEFAULT;	// Monitor memory usage - if free memory falls below threshold, insert batch of records already loaded (default: 10000000)
		generateQaSamples = GENERATE_QASAMPLES_DEFAULT;		// Generate QA Samples at end of ETL (default: False)
		qaSampleProbability = QA_SAMPLE_PROBABILITY_DEFAULT;// The sample probability value used to include a patient's records in the QA Sample export (default: 0.0001)
		memoryLogFile = 0;									// 0: No, 1: every time a batch is inserted in target, 2: for every patient record (default: 0)

		File f = new File("imasis.ini");
		if(f.exists() && !f.isDirectory()) { 
			try {
				settings = new IniFile("imasis.ini");

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

				try {
					Long numParam = Long.valueOf(settings.get("memoryLogFile"));
					if (numParam != null)
						memoryLogFile = numParam;
				} catch (Exception e) {
					memoryLogFile = 0;
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

				String hello = settings.get("hello");
				if (hello != null)
					StringUtilities.outputWithTime(hello);

			} catch (Exception e) {
				StringUtilities.outputWithTime("No .ini file found");
			}
		}
	}

	//********************  ********************//
	private Long getLongValue(String val) {
		if ((val == null) || (val.equals("") || !StringUtilities.isNumber(val)))
			return null;
		else
			return Long.valueOf(val);
	}
	//********************  ********************//
	private Integer getIntValue(String val) {
		if ((val == null) || (val.equals("") || !StringUtilities.isNumber(val)))
			return null;
		else
			return Integer.valueOf(val);
	}
	//********************  ********************//
	private Double getNumValue(String val) {
		if ((val == null) || (val.equals("") || !StringUtilities.isNumber(val)))
			return null;
		else
			return Double.valueOf(val);
	}

	//******************** processPatientSourceRecord ********************//
	private void processPatientSourceRecord(Row row) {
		String tmpS = row.get("patient_id").replaceAll("\u0000", "");
		if ((tmpS.length() > 0) && (StringUtilities.isNumber(tmpS))) {
			etlReport.registerIncomingData("patient", row);
			personId = getLongValue(tmpS);
			personCount++;

			long measureCnt = 0;
			long pressMeasureCnt = 0;
			long labOccurCnt = 0;
			long observationPeriodCnt = 0;
			long conditionCnt = 0;
			long procedureCnt = 0;
			long visitCnt = 0;
			long visitAnnotationCnt = 0;
			long drugExposureCnt = 0;
			if (generateQaSamples)
				qcSampleConstructor.registerPersonData("patient", row, personId);

			if (addPersonAndDeathRecords(row)) {
				measureCnt = processPatientMeasures(row);
				pressMeasureCnt = processPatientPressureMeasures(row);
				labOccurCnt = processLaboratory(row);
				observationPeriodCnt = processVisitPeriodRecords(row);
				conditionCnt = processDiagnosis(row);
				procedureCnt = processProcedures(row);
				visitCnt = processVisitAndRelatedRecords(row);
				visitAnnotationCnt = processVisitAnnotationsRecords(row);
				drugExposureCnt = processDrugRecords(row);
			}
		}
	}

	//********************  ********************//
	private void populateReferenceTables() {
		Long geoCnt = processGeographicalNameRecords();
		Long hospCnt= processVisitHospitalRecords();
		processServiceRecords();
	}

	//******************** PERSON ********************//

	private boolean addPersonAndDeathRecords(Row row) {
		Row person = new Row();
		person.add("person_id", personId);
		String genderSrc = row.get("gender").replaceAll("\u0000", "");
		person.add("gender_concept_id", genderSrc.equals("F") ? "8532" : genderSrc.equals("M") ? "8507" : "8551");
		String dateOfBirth = row.get("birth_date").replaceAll("\u0000", "");  // Date format: YYYY-MM-DD
		if (dateOfBirth.length() < 10) {
			person.add("year_of_birth", "");
			person.add("month_of_birth", "");
			person.add("day_of_birth", "");
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
					}
					else {
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
		person.add("race_concept_id", 8552);	// Unknown (race)
		person.add("ethnicity_concept_id", 0); // No matching concept
		Long locId = geographicalNameToProviderId.get(row.get("birth_place").replaceAll("\u0000", ""));
		person.add("location_id", (locId != null ? locId.toString() : ""));
		person.add("provider_id", "");
		person.add("care_site_id", "");
		person.add("person_source_value", Long.toString(personId));
		person.add("gender_source_value", checkStringLength(genderSrc, 50, "gender_source_value"));
		person.add("gender_source_concept_id", "");
		person.add("race_source_value", "");
		person.add("race_source_concept_id", "");
		person.add("ethnicity_source_value", "");
		person.add("ethnicity_source_concept_id", "");
		tableToRows.put("person", person);

		// ** CREATE death record, if death is indicated
		String ageAtDeath = row.get("death_age").replaceAll("\u0000", "");
		if ((ageAtDeath.length() > 0) && (StringUtilities.isNumber(ageAtDeath)) && (dateOfBirth.length() == 10)) {
			String deathDate = addDaysToDate(dateOfBirth, (365 * Integer.valueOf(ageAtDeath)));
			Row death = new Row();
			death.add("person_id", personId);
			death.add("death_date", deathDate);
			death.add("death_type_concept_id", 38003569); // EHR record patient status "Deceased"
			death.add("cause_concept_id", "");
			death.add("cause_source_value", "");
			death.add("cause_source_concept_id", "");
			tableToRows.put("death", death);
		}

		return true;
	}

	//********************************************************************************//
	private long processPatientMeasures(Row row) {
		String patientId = row.get("patient_id").replaceAll("\u0000", "");
		long numRecs = 0;
		for (Row measureRow : sourceConnection.query("select * from patient_measures where patient_id = '" + patientId + "'")) {
			etlReport.registerIncomingData("patient_measures", measureRow);

			String measureDate = measureRow.get("measurement_date").replaceAll("\u0000", "");
			Long visitOccId = null;          //TODO: visit_occurrence_id !!!!!!!!!!!!!!!!!!!!!!
			if (StringUtilities.isDate(measureDate)) {
				Long tmpL = null;

				String tmpS = measureRow.get("weight").replaceAll("\u0000", "");
				Double weight = getNumValue(tmpS);
				if (weight != null) {
					tmpL = addToMeasurement(personId, (long) 3025315, measureDate, measureDate, 44818701, // 3025315: weight, 44818701: from physical examination
							weight, null, (long) 9529, null, visitOccurrenceId, "weight", null, tmpS);  // 9529: kilogram
					s2t.add(measureRow.get("patient_measure_id").replaceAll("\u0000", ""),// srcInstance
							"patient_measures",		// srcTable
							"weight",				// srcCode
							"", 					// srcName
							"", 					// srcVocabulary
							null, 					// srcConceptId
							"measurement",			// trgTable
							"29463-7",				// trgCode
							"Body weight",			// trgName
							"Measurement",			// trgDomain
							"LOINC", 				// trgVocabulary
							(long) 3025315);		// trgConceptId
					numRecs++;
				}
				tmpS = measureRow.get("height").replaceAll("\u0000", "");
				Double height = getNumValue(tmpS);
				if (height != null) {
					tmpL = addToMeasurement(personId, (long) 3036277, measureDate, measureDate, 44818701, // 3036277: height, 44818701: from physical examination
							height, null, (long) 8582, null, visitOccurrenceId, "height", null, tmpS);  // 8582: centimeter
					s2t.add(measureRow.get("patient_measure_id").replaceAll("\u0000", ""),// srcInstance
							"patient_measures",		// srcTable
							"height",				// srcCode
							"", 					// srcName
							"", 					// srcVocabulary
							null, 					// srcConceptId
							"measurement",			// trgTable
							"8302-2",				// trgCode
							"Body height",			// trgName
							"Measurement",			// trgDomain
							"LOINC", 				// trgVocabulary
							(long) 3036277);		// trgConceptId
					numRecs++;
				}
				tmpS = measureRow.get("bmi").replaceAll("\u0000", "");
				Double imc = getNumValue(tmpS);
				if (imc != null) {
					tmpL = addToMeasurement(personId, (long) 3038553, measureDate, measureDate, 44818701, // 3038553: body mass index, 44818701: from physical examination
							imc, null, (long) 9531, null, visitOccurrenceId, "bmi", null, tmpS);  // 9531: kg/m2
					s2t.add(measureRow.get("patient_measure_id").replaceAll("\u0000", ""),// srcInstance
							"patient_measures",		// srcTable
							"bmi",					// srcCode
							"", 					// srcName
							"", 					// srcVocabulary
							null, 					// srcConceptId
							"measurement",			// trgTable
							"39156-5",				// trgCode
							"Body mass index",		// trgName
							"Measurement",			// trgDomain
							"LOINC", 				// trgVocabulary
							(long) 3038553);		// trgConceptId
					numRecs++;
				}			
			}
		}
		return numRecs;
	}

	//********************************************************************************//
	private long processPatientPressureMeasures(Row row) {
		String patientId = row.get("patient_id").replaceAll("\u0000", "");
		long numRecs = 0;
		for (Row measureRow : sourceConnection.query("select * from patient_pressure_measures where patient_id = '" + patientId + "'")) {
			etlReport.registerIncomingData("patient_pressure_measures", measureRow);
			String measureDate = measureRow.get("measurement_date").replaceAll("\u0000", "");
			String measureTime = measureRow.get("measurement_hour").replaceAll("\u0000", "");
			String measureDateTime = measureDate;
			if (measureTime != null)
				measureDateTime += " " + measureTime;
			Long visitOccId = null;          //TODO: visit_occurrence_id !!!!!!!!!!!!!!!!!!!!!!
			if (StringUtilities.isDate(measureDate)) {
				Long tmpL = null;

				String tmpS = measureRow.get("systolic_pressure").replaceAll("\u0000", "");
				Double systolic = getNumValue(tmpS);
				if (systolic != null) {
					tmpL = addToMeasurement(personId, (long) 3004249, measureDate, measureDateTime, 44818701, // 3004249 : BP systolic, 44818701: from physical examination
							systolic, null, (long) 8876, null, visitOccurrenceId, "systolic_pressure", null, tmpS);  // 8876: millimeter mercury column
					s2t.add(measureRow.get("patient_pressure_measure_id").replaceAll("\u0000", ""), // srcInstance
							"patient_pressure_measures",// srcTable
							"systolic_pressure",	// srcCode
							"", 					// srcName
							"", 					// srcVocabulary
							null, 					// srcConceptId
							"measurement",			// trgTable
							"8480-6",				// trgCode
							"BP systolic",			// trgName
							"Measurement",			// trgDomain
							"LOINC", 				// trgVocabulary
							(long) 3004249);		// trgConceptId
					numRecs++;
				}
				tmpS = measureRow.get("diastolic_pressure").replaceAll("\u0000", "");
				Double diastolic = getNumValue(tmpS);
				if (diastolic != null) {
					tmpL = addToMeasurement(personId, (long) 3012888, measureDate, measureDateTime, 44818701, // 3012888: BP diastolic, 44818701: from physical examination
							diastolic, null, (long) 8876, null, visitOccurrenceId, "diastolic_pressure", null, tmpS);  // 8876: millimeter mercury column
					s2t.add(measureRow.get("patient_pressure_measure_id").replaceAll("\u0000", ""),// srcInstance
							"patient_pressure_measures",// srcTable
							"diastolic_pressure",	// srcCode
							"", 					// srcName
							"", 					// srcVocabulary
							null, 					// srcConceptId
							"measurement",			// trgTable
							"8462-4",				// trgCode
							"BP diastolic",			// trgName
							"Measurement",			// trgDomain
							"LOINC", 				// trgVocabulary
							(long) 3012888);		// trgConceptId
					numRecs++;
				}
			}
		}
		return numRecs;
	}

	//********************************************************************************//
	private long processVisitAndRelatedRecords(Row row) {
		String patientId = row.get("patient_id").replaceAll("\u0000", "");
		long numRecs = 0;

		for (Row visitRow : sourceConnection.query("select * from visit where patient_id = '" + patientId + "'")) {
			String startDate = visitRow.get("start_date").replaceAll("\u0000", "");
			String endDate = visitRow.get("end_date").replaceAll("\u0000", "");
			if (StringUtilities.isDate(startDate)) {
				etlReport.registerIncomingData("visit", visitRow);
				Long refCareSiteId = null;
				String hospital = visitRow.get("visit_hospital_id").replaceAll("\u0000", "");
				if (hospital.length() > 0) {
					refCareSiteId = sourceToCareSiteId.get(hospital);
				}

				Long refProviderId = visitRow.getLong("service_id");
				long visitConceptId = 9202; // Outpatient visit
				String tmpS = visitRow.get("visit_id").replaceAll("\u0000", "");
				if ((tmpS.length() > 0) && (StringUtilities.isNumber(tmpS))) {
					Long visitid = getLongValue(tmpS);
					String visittype = visitRow.get("visit_type_id").replaceAll("\u0000", "");
					if (visittype.length() > 0) {
						switch (visittype.toLowerCase()) {
						case "1":  // Inpatient visit
							visitConceptId = 9201; // Inpatient visit 
							break;
						case "2":   // Major ambulatory surgery
							visitConceptId = 9202; // Outpatient Visit 
							break;
						case "3":   // Outpatient Visit
							visitConceptId = 9202; // Outpatient Visit 
							break;
						case "4":   // Emergency Room Visit
							visitConceptId = 9203; // Emergency Room Visit 
							break;
						default:
							// TODO 
						}
					}
					addToVisitOccurrence(visitid, startDate, endDate, visitConceptId, (long) 44818518, // 44818518: Visit derived from EHR record
							refCareSiteId, refProviderId, tmpS);
					numRecs++;
				}
			}
		}

		return numRecs;
	}

	//********************************************************************************//
	private long processDrugRecords(Row row) {
		String patientId = row.get("patient_id").replaceAll("\u0000", "");
		long numRecs = 0;

		// DRUGS 
		String sqlQ = "select da.drug_administration_id, da.drug_id, da.administration_day,  ";
		sqlQ += "da.administration_hour, da.dosage, d.national_code_cima_id, d.administration_code \n";
		sqlQ += "from drug_administration da, drug d \n";
		sqlQ += "where da.patient_id = " + patientId +" \n";
		sqlQ += "and d.drug_id = da.drug_id;";
		for (Row drugRow : sourceConnection.query(sqlQ)) {
			etlReport.registerIncomingData("drug_administration", drugRow);
			String drugAdministrationId = drugRow.get("drug_administration_id").replaceAll("\u0000", "");
			String drugId = drugRow.get("drug_id").replaceAll("\u0000", "");
			String administrationDay = drugRow.get("administration_day").replaceAll("\u0000", "");
			String administrationHour = drugRow.get("administration_hour").replaceAll("\u0000", "");
			String administrationDayTime = administrationDay;
			if ((administrationHour != null) && (administrationHour.length() > 7))
				administrationDayTime += " " + administrationHour;

			String tmpS = drugRow.get("dosage").replaceAll("\u0000", "");
			Double dosage = getNumValue(tmpS);

			String administrationCode = drugRow.get("administration_code").replaceAll("\u0000", "").trim().toLowerCase();
			Long routeConceptId = null;
			if (administrationCode.length() > 0) 
				routeConceptId = adminCodeToConceptId.get(administrationCode);

			tmpS = drugRow.get("national_code_cima_id").replaceAll("\u0000", "");	
			if ((tmpS.length() > 0) && (StringUtilities.isNumber(tmpS))) {
				Long natCode = getLongValue(tmpS);
				CodeDomainData srcData = natCodeCimaIdToConcept.getCodeData(tmpS);
				for (TargetConcept targetConcept : srcData.targetConcepts) {
					numRecs++;
					addToDrugExposure(
							getLongValue(drugAdministrationId),	// drug_exposure_id
							personId,							// person_id
							targetConcept.conceptId,			// drug_concept_id
							administrationDay,					// drug_exposure_start_date
							administrationDayTime,				// drug_exposure_start_datetime
							administrationDay,					// drug_exposure_end_date
							administrationDayTime,				// drug_exposure_end_datetime
							null, 								// verbatim_end_date
							(long) 38000180, 					// drug_type_concept_id, 38000180: Inpatient administration
							null,								// stop_reason
							null,								// refills
							dosage,								// quantity
							null,								// days_supply
							null,								// sig
							routeConceptId,						// route_concept_id
							null,								// lot_number
							null,								// provider_id
							null,								// visit_occurrence_id
							Long.toString(natCode),				// drug_source_value
							null,								// drug_source_concept_id
							administrationCode,					// route_source_value
							null);								// dose_unit_source_value
					s2t.add(drugAdministrationId, 				// srcInstance
							"drug_administration",				// srcTable
							Long.toString(natCode), 			// srcCode
							srcData.sourceConceptName,			// srcName
							"", 								// srcVocabulary
							null, 								// srcConceptId
							"drug_exposure",					// trgTable
							targetConcept.conceptCode,			// trgCode
							targetConcept.conceptName,			// trgName
							"Drug",								// trgDomain
							"RxNorm", 							// trgVocabulary
							(long) targetConcept.conceptId);	// trgConceptId
				}
			} else {  // no national_code_cima_id provided
				s2t.add(drugAdministrationId, 	// srcInstance
						"drug_administration",	// srcTable
						null, 					// srcCode
						"n/a",					// srcName
						"", 					// srcVocabulary
						null, 					// srcConceptId
						"drug_exposure",		// trgTable
						"n/a",					// trgCode
						"n/a",					// trgName
						"n/a",					// trgDomain
						"n/a", 					// trgVocabulary
						null);					// trgConceptId
			}
		}

		return numRecs;
	}

	//********************************************************************************//
	private long getConditionTypeConceptId(String diagtype) {
		long condTypeConceptId = 0; 
		Long conceptId = diagnosisCodeToConceptId.get(diagtype);
		if (conceptId != null)
			condTypeConceptId = conceptId;

		return condTypeConceptId;
	}

	//********************************************************************************//
	private long getProcedureTypeConceptId(String proctype) {
		long procTypeConceptId = 0;
		Long conceptId = procedureCodeToConceptId.get(proctype);
		if (conceptId != null)
			procTypeConceptId = conceptId;

		return procTypeConceptId;
	}

	//********************************************************************************//
	private String getVocabularyVersion() {
		String vocabVersion = "";
		for (Row row : targetConnection.query("select * from vocabulary where vocabulary_id='None'")) {
			vocabVersion = row.get("vocabulary_version");
			break;
		}
		return vocabVersion;
	}

	//********************************************************************************//
	private Long lookupServiceToProviderId(String refStr) {
		Long retProviderId = null;
		if ((refStr != null) && StringUtilities.isNumber(refStr)) {
			retProviderId = serviceToProviderId.get(getLongValue(refStr));
		}
		return retProviderId;
	}

	//********************************************************************************//
	private long processVisitPeriodRecords(Row row) {
		String patientId = row.get("patient_id").replaceAll("\u0000", "");
		long numRecs = 0;
		for (Row periodRow : sourceConnection.query("select * from visit_period where patient_id = '" + patientId + "'")) {
			etlReport.registerIncomingData("visit_period", periodRow);
			Long visitPeriodId = periodRow.getLong("visit_period_id");
			String startDate = periodRow.get("start_date").replaceAll("\u0000", "");
			String endDate = periodRow.get("end_date").replaceAll("\u0000", "");
			if (StringUtilities.isDate(startDate)){
				Row observationPeriod = new Row();
				observationPeriod.add("observation_period_id", visitPeriodId);
				observationPeriod.add("person_id", personId);
				observationPeriod.add("observation_period_start_date", startDate);
				observationPeriod.add("observation_period_start_datetime", startDate);
				if (StringUtilities.isDate(endDate)) {
					observationPeriod.add("observation_period_end_date", endDate);
					observationPeriod.add("observation_period_end_datetime", endDate);
				}
				else {
					observationPeriod.add("observation_period_end_date", startDate);
					observationPeriod.add("observation_period_end_datetime", startDate);
				}
				observationPeriod.add("period_type_concept_id", 44814724); // Period covering healthcare encounters
				tableToRows.put("observation_period", observationPeriod);
				numRecs++;
			}
		}
		return numRecs;
	}
	//********************************************************************************//
	private long processVisitAnnotationsRecords(Row row) {
		String patientId = row.get("patient_id").replaceAll("\u0000", "");
		long numRecs = 0;
		for (Row annotationRow : sourceConnection.query("select * from visit_annotations where patient_id = '" + patientId + "'")) {
			etlReport.registerIncomingData("visit_annotations", annotationRow);
			Long refProviderId = lookupServiceToProviderId(annotationRow.get("service_id").replaceAll("\u0000", ""));
			String tmpS = annotationRow.get("visit_annotations_id").replaceAll("\u0000", "");
			Long visitAnnotationsId = null;
			if ((tmpS != null) && StringUtilities.isNumber(tmpS)) {
				visitAnnotationsId = getLongValue(tmpS);
				String noteText = annotationRow.get("annotation").replaceAll("\u0000", "");
				String annotationDate = annotationRow.get("annotation_date").replaceAll("\u0000", "");
				String annotationTime = annotationRow.get("annotation_hour").replaceAll("\u0000", "");
				String annotationDateTime = annotationDate;
				if ((annotationTime != null) && (annotationTime.length()>7))
					annotationDateTime += " " + annotationTime;
				tmpS = annotationRow.get("visit_period_id").replaceAll("\u0000", "");
				Long visitId = getLongValue(tmpS);
				if (StringUtilities.isDate(annotationDate)) {
					Row note = new Row();
					note.add("note_id", visitAnnotationsId);
					note.add("person_id", personId);
					note.add("note_date", annotationDate);
					note.add("note_datetime", annotationDateTime);
					note.add("note_type_concept_id", 44814645); // Note
					note.add("note_class_concept_id", 0); //TODO: Check
					note.add("note_title", "");
					note.add("note_text", noteText);
					note.add("encoding_concept_id", 0);//TODO: Check
					note.add("language_concept_id", 0);//TODO: Check
					note.add("provider_id", (refProviderId != null ? refProviderId.toString() : ""));
					note.add("visit_occurrence_id", (visitId != null ? visitId.toString() : ""));
					note.add("note_source_value", visitAnnotationsId.toString());
					tableToRows.put("note", note);
				}
				numRecs++;
			}
		}
		return numRecs;
	}

	//********************************************************************************//
	private long processDiagnosis(Row row) {
		String patientId = row.get("patient_id").replaceAll("\u0000", "");
		long numRecs = 0;
		String qry = "select d.diagnosis_id, d.visit_id, d.patient_id, dt.diagnosis_type, d.diagnosis_date, ";
		qry += "d.icd9cm_code_id from diagnosis d \n";
		qry += "left join diagnosis_type dt on dt.diagnosis_type_id=d.diagnosis_type_id\n";
		qry += "where d.patient_id = " + patientId;
		for (Row diagnosisRow : sourceConnection.query(qry)) {
			etlReport.registerIncomingData("diagnosis", diagnosisRow);
			String diagnosisId = diagnosisRow.get("diagnosis_id").replaceAll("\u0000", "");
			String code = diagnosisRow.get("icd9cm_code_id").replaceAll("\u0000", "");
			String diagtype = diagnosisRow.get("diagnosis_type").replaceAll("\u0000", "");
			String diagDate = diagnosisRow.get("diagnosis_date").replaceAll("\u0000", "");
			String tmpS = diagnosisRow.get("visit_id").replaceAll("\u0000", "");
			Long visitId = getLongValue(tmpS);
			CodeDomainData data = icd9ToConcept.getCodeData(code);
			if (data != null) {
				for (TargetConcept targetConcept : data.targetConcepts) {
					long tmpId = 0;
					String targetDomain = targetConcept.domainId;
					switch (targetDomain) {
					case "Condition":
						tmpId = addToConditionOccurrence(
								personId,						// person_id
								(long) targetConcept.conceptId,	// condition_concept_id
								diagDate,						// condition_start_date
								null, 							// condition_end_date
								getConditionTypeConceptId(diagtype),// condition_type_concept_id
								null,							// stop_reason
								null,							// provider_id
								visitId,						// coVisit_occurrence_id
								code,							// condition_source_value
								(long) data.sourceConceptId);	// condition_source_concept_id
						s2t.add(diagnosisId, 					// srcInstance
								"diagnosis",					// srcTable
								code,							// srcCode
								data.sourceConceptName,			// srcName
								"ICD9CM", 						// srcVocabulary
								(long) data.sourceConceptId,	// srcConceptId
								"condition",					// trgTable
								targetConcept.conceptCode,		// trgCode
								targetConcept.conceptName,		// trgName
								targetDomain,					// trgDomain
								"SNOMED", 						// trgVocabulary
								(long) targetConcept.conceptId);// trgConceptId

						break;

					case "Measurement":
						tmpId = addToMeasurement(
								personId,						// person_id
								(long) targetConcept.conceptId,	// measurement_concept_id
								diagDate,						// measurement_date
								diagDate,						// measurement_datetime
								(long) 44818702, 				// measurement_type_concept_id, 44818702: Lab result
								null,							// operator_concept_id
								null,							// value_as_number
								null,							// value_as_conceptId
								null,							// unit_concept_id
								null,							// range_low
								null, 							// range_high
								null, 							// provider_id
								visitId, 						// visit_occurrence_id
								code, 							// measurement_source_value
								(long) data.sourceConceptId,	// measurement_source_concept_id
								null,							// unit_source_value
								null);							// value_source_value
						s2t.add(diagnosisId, 					// srcInstance
								"diagnosis",					// srcTable
								code,							// srcCode
								data.sourceConceptName,			// srcName
								"ICD9CM", 						// srcVocabulary
								(long) data.sourceConceptId,	// srcConceptId
								"measurement",					// trgTable
								targetConcept.conceptCode,		// trgCode
								targetConcept.conceptName,		// trgName
								targetDomain,					// trgDomain
								"SNOMED", 						// trgVocabulary
								(long) targetConcept.conceptId);// trgConceptId
						break;

					case "Observation":
						tmpId = addToObservation(
								personId, 						// person_id
								(long) targetConcept.conceptId, // observation_concept_id
								diagDate, 						// observation_date
								null, 							// observation_datetime
								(long) 38000280, 				// observation_type_concept_id, 38000280: Observation recorded from EHR
								null,							// value_as_number
								null,							// value_as_string
								null,							// value_as_concept_id
								null,							// qualifier_concept_id
								null,							// unit_concept_id
								null,							// provider_id
								visitId,						// visit_occurrence_id
								code,							// observation_source_value
								(long) data.sourceConceptId,	// observation_source_concept_id
								null,							// unit_source_value
								null);							// qualifier_source_value
						s2t.add(diagnosisId, 					// srcInstance
								"diagnosis",					// srcTable
								code,							// srcCode
								data.sourceConceptName,			// srcName
								"ICD9CM", 						// srcVocabulary
								(long) data.sourceConceptId,	// srcConceptId
								"observation",					// trgTable
								targetConcept.conceptCode,		// trgCode
								targetConcept.conceptName,		// trgName
								targetDomain,					// trgDomain
								"SNOMED", 						// trgVocabulary
								(long) targetConcept.conceptId);// trgConceptId
						break;

					case "Procedure":
						tmpId = addToProcedureOccurrence(
								personId,						// person_id
								(long) targetConcept.conceptId,	// procedure_concept_id
								diagDate,						// procedure_date
								diagDate,						// procedure_datetime
								(long) 44818701, 				// procedure_type_concept_id, 44818701: From physical examination
								null,							// modifier_concept_id
								null,							// quantity
								null,							// provider_id
								visitId,						// visit_occurrence_id
								code, 							// procedure_source_value
								(long) data.sourceConceptId,	// procedure_source_concept_id
								null);							// qualifierSourceValue
						s2t.add(diagnosisId, 					// srcInstance
								"diagnosis",					// srcTable
								code,							// srcCode
								data.sourceConceptName,			// srcName
								"ICD9CM", 						// srcVocabulary
								(long) data.sourceConceptId,	// srcConceptId
								"procedure",					// trgTable
								targetConcept.conceptCode,		// trgCode
								targetConcept.conceptName,		// trgName
								targetDomain,					// trgDomain
								"SNOMED", 						// trgVocabulary
								(long) targetConcept.conceptId);// trgConceptId
						break;

					default:
						System.out.println("Other domain from 'diagnosis'"+":"+targetDomain+", code: "+code+", concept_id"+targetConcept.conceptId+", "+personId); 
						etlReport.reportProblem("condition_occurrence", "Other domain from 'diagnosis_occur'"+":"+targetDomain+", code: "+code+", concept_id: "+targetConcept.conceptId, Long.toString(personId));
						s2t.add(diagnosisId, 					// srcInstance
								"diagnosis",					// srcTable
								code,							// srcCode
								data.sourceConceptName,			// srcName
								"ICD9CM", 						// srcVocabulary
								(long) data.sourceConceptId,	// srcConceptId
								"n/a",					// trgTable
								targetConcept.conceptCode,		// trgCode
								targetConcept.conceptName,		// trgName
								targetDomain,					// trgDomain
								"SNOMED", 						// trgVocabulary
								(long) targetConcept.conceptId);// trgConceptId
						break;
					}
					numRecs++;
				}
			} else { // lookup of code failed
				s2t.add(diagnosisId, 					// srcInstance
						"diagnosis",					// srcTable
						code,							// srcCode
						"<icd9cm_code_id>",				// srcName
						"n/a", 							// srcVocabulary
						null,							// srcConceptId
						"n/a",							// trgTable
						"n/a",							// trgCode
						"n/a",							// trgName
						"n/a",							// trgDomain
						"n/a", 							// trgVocabulary
						null);							// trgConceptId
			}
		}
		return numRecs;
	}

	//********************************************************************************//
	private long processProcedures(Row row) {
		String patientId = row.get("patient_id").replaceAll("\u0000", "");
		long numRecs = 0;
		String qry = "select p.procedures_id, p.patient_id, p.visit_id, p.procedure_code, pt.procedure_type, v.end_date, v.service_id from procedures p\n";
		qry += "left join visit v on p.visit_id=v.visit_id\n";
		qry += "left join procedure_type pt on p.procedure_type_id=pt.procedure_type_id\n";
		qry += "where p.patient_id = " + patientId;
		for (Row procedureRow : sourceConnection.query(qry)) {
			etlReport.registerIncomingData("procedures", procedureRow);
			long procOccurId = procedureRow.getLong("procedures_id");
			String code = procedureRow.get("procedure_code").replaceAll("\u0000", "");
			String proctype = procedureRow.get("procedure_type").replaceAll("\u0000", "");
			String tmpS = procedureRow.get("visit_id").replaceAll("\u0000", "");
			Long visitId = getLongValue(tmpS);
			String endDate = procedureRow.get("end_date").replaceAll("\u0000", "");
			tmpS = procedureRow.get("service_id").replaceAll("\u0000", "");
			Long refProviderId = lookupServiceToProviderId(tmpS);

			CodeDomainData data = icd9ProcToConcept.getCodeData(code);
			if (data != null) {
				for (TargetConcept targetConcept : data.targetConcepts) {
					String targetDomain = targetConcept.domainId;
					switch (targetDomain) {
					case "Procedure":
						addToProcedureOccurrence(
								procOccurId,						// procedure_occurrence_id
								personId, 							// person_id
								(long) targetConcept.conceptId, 	// procedure_concept_id
								endDate, 							// procedure_date
								endDate,							// procedure_datetime
								getProcedureTypeConceptId(proctype),// procedure_type_concept_id
								null, 								// modifier_concept_id
								null, 								// quantity
								refProviderId, 						// provider_id
								visitId, 							// visit_occurrence_id
								code, 								// procedure_source_vale
								(long) data.sourceConceptId, 		// procedure_source_concept_id
								null);								// qualifier_source_value
						s2t.add(Long.toString(procOccurId),			// srcInstance
								"procedures",						// srcTable
								code,								// srcCode
								data.sourceConceptName,				// srcName
								"ICD9Proc", 						// srcVocabulary
								(long) data.sourceConceptId,		// srcConceptId
								"procedure",						// trgTable
								targetConcept.conceptCode,			// trgCode
								targetConcept.conceptName,			// trgName
								targetDomain,						// trgDomain
								"ICD9Proc", 						// trgVocabulary
								(long) targetConcept.conceptId);	// trgConceptId
						break;

					default:
						System.out.println("Other domain from 'procedure'"+":"+targetDomain+", code: "+code+", concept_id"+targetConcept.conceptId+", "+personId); 
						etlReport.reportProblem("procedure_occurrence", "Other domain from 'procedure'"+":"+targetDomain+", code: "+code+", concept_id: "+targetConcept.conceptId, Long.toString(personId));
						s2t.add(Long.toString(procOccurId),			// srcInstance
								"procedures",						// srcTable
								code,								// srcCode
								data.sourceConceptName,				// srcName
								"ICD9Proc", 						// srcVocabulary
								(long) data.sourceConceptId,		// srcConceptId
								"n/a",								// trgTable
								targetConcept.conceptCode,			// trgCode
								targetConcept.conceptName,			// trgName
								targetDomain,						// trgDomain
								"ICD9Proc", 						// trgVocabulary
								(long) targetConcept.conceptId);	// trgConceptId
						break;
					}
					numRecs++;
				}
			} else { // lookup of code failed
				s2t.add(Long.toString(procOccurId), 	// srcInstance
						"procedures",					// srcTable
						code,							// srcCode
						"<procedure_code>",				// srcName
						"n/a", 							// srcVocabulary
						null,							// srcConceptId
						"n/a",							// trgTable
						"n/a",							// trgCode
						"n/a",							// trgName
						"n/a",							// trgDomain
						"n/a", 							// trgVocabulary
						null);							// trgConceptId
			}
		}
		return numRecs;
	}

	//********************************************************************************//
	private long processLaboratory(Row row) {
		String patientId = row.get("patient_id").replaceAll("\u0000", "");
		long numRecs = 0;

		String sqlQ = "select l.laboratory_id, l.lab_test_name_id, l.lab_result_name_id, l.request_date, ";
		sqlQ += "l.result, l.lab_measure_unit_SI_id, l.visit_period_id \n";
		sqlQ += "from laboratory l \n";
		sqlQ += "where l.patient_id=" + patientId +";";

		for (Row laboratoryRow : sourceConnection.query(sqlQ)) {
			etlReport.registerIncomingData("laboratory", laboratoryRow);
			String laboratoryId = laboratoryRow.get("laboratory_id").replaceAll("\u0000", "");
			String labTestNameId = laboratoryRow.get("lab_test_name_id").replaceAll("\u0000", "").trim();
			String labResultNameId = laboratoryRow.get("lab_result_name_id").replaceAll("\u0000", "").trim();
			String requestDate = laboratoryRow.get("request_date").replaceAll("\u0000", "").trim();
			String tmpS = laboratoryRow.get("visit_period_id").replaceAll("\u0000", "");
			Long visitId = getLongValue(tmpS);

			String nameUnime = laboratoryRow.get("lab_measure_unit_SI_id").replaceAll("\u0000", "").trim();
			Long unitConceptId = null;
			if (nameUnime.length() > 0)
				unitConceptId = labmeasureunitToConceptId.get(nameUnime.toLowerCase());

			String result = replaceSpecialChars(laboratoryRow.get("result").replaceAll("\u0000", "").trim());
			Double resultVal = null;
			String resultStr = null;
			Long resultConceptId = null;
			Long operatorConceptId = null;
			if (result.length() > 0) {
				if(StringUtilities.isNumber(result))
					resultVal = getNumValue(result);
				else {
					if ((result.startsWith("<=")) || (result.startsWith(">="))) {
						operatorConceptId = extractOperatorConceptId(result);
						try {
							resultVal = Double.parseDouble(result.substring(2));
						} catch (Exception e) {
							resultVal = null;
							resultStr = result.substring(2);
						}
					}
					else {
						if ((result.startsWith("<")) || (result.startsWith(">"))|| (result.startsWith("="))) {
							operatorConceptId = extractOperatorConceptId(result);
							try {
								resultVal = Double.parseDouble(result.substring(1));
							} catch (Exception e) {
								resultVal = null;
								resultStr = result.substring(1);
							}
						}
						else {
							resultConceptId = resultcodeToConceptId.get(result.toLowerCase());
							if (resultConceptId == null)
								resultStr = result;
						}
					}
				}
			}
			String resTestName = labResultNameId + "_" + labTestNameId;
			String loincId = resTestToLoincCode.get(resTestName);
			if ((loincId != null) && (loincId.length() > 0)) {
				CodeDomainData data = loincToConcept.getCodeData(loincId);
				for (TargetConcept targetConcept : data.targetConcepts) {
					Long measConceptId = (long) targetConcept.conceptId;
					String domainId = targetConcept.domainId;

					Long tmpId = null;
					if (domainId != null) {
						switch (domainId) {
						case "Measurement":
							tmpId = addToMeasurement(
									personId,			// person_id
									measConceptId,		// measurement_concept_id
									requestDate,		// measurement_date
									requestDate,		// measurement_datetime
									(long) 44818702, 	// measurement_type_concept_id, 44818702: Lab result
									operatorConceptId,	// operator_concept_id
									resultVal,			// value_as_number
									resultConceptId,	// value_as_conceptId
									unitConceptId,		// unit_concept_id
									null,				// range_low
									null, 				// range_high
									null, 				// provider_id
									visitId, 			// visit_occurrence_id
									labTestNameId, 		// measurement_source_value
									null,				// measurement_source_concept_id
									nameUnime,			// unit_source_value
									result);			// value_source_value
							s2t.add(laboratoryId, 					// srcInstance
									"laboratory",					// srcTable
									loincId,						// srcCode
									data.sourceConceptName,			// srcName
									"LOINC", 						// srcVocabulary
									(long) data.sourceConceptId,	// srcConceptId
									"measurement",					// trgTable
									targetConcept.conceptCode,		// trgCode
									targetConcept.conceptName,		// trgName
									domainId,						// trgDomain
									"LOINC", 						// trgVocabulary
									(long) targetConcept.conceptId);// trgConceptId
							numRecs++;
							break;
						case "Observation":
							tmpId = addToObservation(
									personId,				// person_id
									measConceptId,			// observation_concept_id
									requestDate,			// observation_date
									null,					// observation_datetime
									(long) 38000280,		// observation_type_concept_id, 38000280: Observation recorded from EHR
									resultVal,				// value_as_number
									resultStr,				// value_as_string
									resultConceptId,		// value_as_concept_id
									operatorConceptId,		// qualifier_concept_id
									unitConceptId,			// unit_concept_id
									null,					// provider_id
									null,					// visit_occurrence_id
									result,					// observation_source_value
									null,					// observation_source_concept_id
									nameUnime,				// unitSource_value
									null);					// qualifier_source_value
							s2t.add(laboratoryId, 					// srcInstance
									"laboratory",					// srcTable
									loincId,						// srcCode
									data.sourceConceptName,			// srcName
									"LOINC", 						// srcVocabulary
									(long) data.sourceConceptId,	// srcConceptId
									"observation",					// trgTable
									targetConcept.conceptCode,		// trgCode
									targetConcept.conceptName,		// trgName
									domainId,						// trgDomain
									"LOINC", 						// trgVocabulary
									(long) targetConcept.conceptId);// trgConceptId
							numRecs++;
							break;
						default:
							if (domainId.length() > 0) {
								System.out.println("Other domain from 'laboratory'"+":"+domainId+", concept_id"+measConceptId+", "+personId); 
								etlReport.reportProblem("measurement", "Other domain from 'laboratory_occur':"+domainId+", concept_id: "+measConceptId, Long.toString(personId));
								s2t.add(laboratoryId, 					// srcInstance
										"laboratory",					// srcTable
										loincId,						// srcCode
										data.sourceConceptName,			// srcName
										"LOINC", 						// srcVocabulary
										(long) data.sourceConceptId,	// srcConceptId
										"n/a",							// trgTable
										targetConcept.conceptCode,		// trgCode
										targetConcept.conceptName,		// trgName
										domainId,						// trgDomain
										"LOINC", 						// trgVocabulary
										(long) targetConcept.conceptId);// trgConceptId
							}
							break;
						}
					} else {
						s2t.add(laboratoryId, 					// srcInstance
								"laboratory",					// srcTable
								loincId,						// srcCode
								data.sourceConceptName,			// srcName
								"LOINC", 						// srcVocabulary
								(long) data.sourceConceptId,	// srcConceptId
								"n/a",							// trgTable
								targetConcept.conceptCode,		// trgCode
								targetConcept.conceptName,		// trgName
								"n/a",							// trgDomain
								"LOINC", 						// trgVocabulary
								(long) targetConcept.conceptId);// trgConceptId
					}
				}
			} else { // no LOINC_ID given
				s2t.add(laboratoryId, 					// srcInstance
						"laboratory",					// srcTable
						resTestName,					// srcCode
						"<resultNameId_testNameId>",	// srcName
						"n/a", 							// srcVocabulary
						null,							// srcConceptId
						"n/a",							// trgTable
						"n/a",							// trgCode
						"n/a",							// trgName
						"n/a",							// trgDomain
						"n/a", 							// trgVocabulary
						null);						// trgConceptId
			}
		}
		return numRecs;
	}

	//********************************************************************************//
	private void processServiceRecords() {
		for (Row serviceRow : sourceConnection.query("select * from services where service_id is not null")) {
			Long serviceId = serviceRow.getLong("service_id");
			String description = serviceRow.get("description").replaceAll("\u0000", "");
			Long refProviderId = serviceToProviderId.get(serviceId);
			if (refProviderId == null) {
				etlReport.registerIncomingData("services", serviceRow);
				serviceToProviderId.put(serviceId, serviceId);
				Row provider = new Row();
				provider.add("provider_id", serviceId);
				provider.add("provider_name", description);  //TODO
				provider.add("gender_concept_id", 8551);  // Unknown
				provider.add("provider_source_value", serviceId);
				tableToRows.put("provider", provider);
			}
		}
		for (Row serviceRow : sourceConnection.query("select distinct(service_id) as service_id from visit")) {
			Long serviceId = serviceRow.getLong("service_id");
			Long refProviderId = serviceToProviderId.get(serviceId);
			if (refProviderId == null) {
				etlReport.registerIncomingData("visit.service_id", serviceRow);
				serviceToProviderId.put(serviceId, serviceId);
				Row provider = new Row();
				provider.add("provider_id", serviceId);
				provider.add("provider_name", ""); 
				provider.add("gender_concept_id", 8551);  // Unknown
				provider.add("provider_source_value", serviceId);
				tableToRows.put("provider", provider);
			}
		}

	}

	//********************************************************************************//
	private long processGeographicalNameRecords() {
		long numRecs = 0;

		for (Row geoRow : sourceConnection.query("select * from geographical_name")) {
			etlReport.registerIncomingData("geographical_name", geoRow);
			String geoNameId = geoRow.get("geographical_name_id").replaceAll("\u0000", "").trim();
			String name = geoRow.get("name").replaceAll("\u0000", "").trim();
			if (geographicalNameToProviderId.get(name) == null) {
				if ((geoNameId.length() > 0) && (StringUtilities.isNumber(geoNameId))) {
					locationId = getLongValue(geoNameId);
					geographicalNameToProviderId.put(name, locationId);
					Row location = new Row();
					location.add("location_id", locationId);
					location.add("address_1", "");
					location.add("address_2", "");
					location.add("city", "");
					location.add("state", "");
					location.add("zip", "");
					location.add("county", "");
					location.add("location_source_value", name);
					tableToRows.put("location", location);
					numRecs++;
				}
			}
		}
		return numRecs;
	}

	//********************************************************************************//
	private long processVisitHospitalRecords() {
		long numRecs = 0;

		for (Row hospRow : sourceConnection.query("select * from visit_hospital")) {
			etlReport.registerIncomingData("hospital", hospRow);
			String visitHospId = hospRow.get("visit_hospital_id").replaceAll("\u0000", "").trim();
			String name = hospRow.get("visit_hospital_label").replaceAll("\u0000", "").trim();
			if (sourceToCareSiteId.get(name) == null ) {
				if ((visitHospId.length() > 0) && (StringUtilities.isNumber(visitHospId))) {
					careSiteId = getLongValue(visitHospId);
					sourceToCareSiteId.put(name, careSiteId);
					Row careSite = new Row();
					careSite.add("care_site_id", careSiteId);
					careSite.add("care_site_name", name);
					careSite.add("care_site_source_value", visitHospId);
					tableToRows.put("care_site", careSite);
					numRecs++;
				}
			}
		}
		return numRecs;
	}

	//********************  ********************//
	private void addToDrugExposure(
			long drugExposureId,
			long dePersonId,
			long drugConceptId,
			String drugExposureStartDate,
			String drugExposureStartDateTime,
			String drugExposureEndDate,
			String drugExposureEndDateTime,
			String verbatimEndDateTime,
			long drugTypeConceptId,
			String stopReason,
			Long refills,
			Double quantity,
			Long daysSupply,
			String sig,
			Long routeConceptId,
			String lotNumber,
			Long deProviderId,
			Long visitOccurrenceId,
			String drugSourceValue,
			Long drug_sourceConceptId,
			String routeSourceValue,
			String doseUnitSourceValue) {
		Row drugExposure = new Row();
		drugExposure.add("drug_exposure_id", drugExposureId);
		drugExposure.add("person_id", dePersonId);
		drugExposure.add("drug_concept_id", drugConceptId);
		drugExposure.add("drug_exposure_start_date", drugExposureStartDate != null ? drugExposureStartDate : "");
		drugExposure.add("drug_exposure_start_datetime", drugExposureStartDateTime != null ? drugExposureStartDateTime : "");
		drugExposure.add("drug_exposure_end_date", drugExposureEndDate != null ? drugExposureEndDate : "");
		drugExposure.add("drug_exposure_end_datetime", drugExposureEndDateTime != null ? drugExposureEndDateTime : "");
		drugExposure.add("verbatim_end_date", verbatimEndDateTime != null ? verbatimEndDateTime : "");
		drugExposure.add("drug_type_concept_id", drugTypeConceptId);
		drugExposure.add("stop_reason", stopReason != null ? stopReason : "");
		drugExposure.add("refills", (refills != null ? refills.toString() : ""));
		drugExposure.add("quantity", (quantity != null ? quantity.toString() : ""));
		drugExposure.add("days_supply", (daysSupply != null ? daysSupply.toString() : ""));
		drugExposure.add("sig", sig != null ? sig : "");
		drugExposure.add("route_concept_id", (routeConceptId != null ? routeConceptId.toString() : ""));
		drugExposure.add("lot_number", lotNumber != null ? lotNumber : "");
		drugExposure.add("provider_id", (deProviderId != null ? deProviderId.toString() : ""));
		drugExposure.add("visit_occurrence_id", (visitOccurrenceId != null ? visitOccurrenceId.toString() : ""));
		drugExposure.add("drug_source_value", drugSourceValue != null ? drugSourceValue : "");
		drugExposure.add("drug_source_concept_id", (drug_sourceConceptId != null ? drug_sourceConceptId.toString() : ""));
		drugExposure.add("route_source_value", routeSourceValue != null ? routeSourceValue : "");
		drugExposure.add("dose_unit_source_value", doseUnitSourceValue != null ? doseUnitSourceValue : "");
		tableToRows.put("drug_exposure", drugExposure);
	}

	//********************  ********************//
	private long addToMeasurement(
			long mPersonId,
			long measurementConceptId,
			String measurementDate,
			String measurementDateTime,
			long measurementTypeConceptId,
			Double valueAsNumber,
			Long valueAsConceptId,
			Long unitConceptId,
			Long mProviderId,
			Long visitOccurrenceId,
			String measurementSourceValue,
			String unitSourceValue,
			String valueSourceValue) {
		return addToMeasurement(mPersonId, measurementConceptId, measurementDate, measurementDateTime, measurementTypeConceptId, null, valueAsNumber, valueAsConceptId, unitConceptId, null, null, mProviderId, visitOccurrenceId, measurementSourceValue, null, unitSourceValue, valueSourceValue);
	}

	//********************  ********************//
	private long addToMeasurement(
			long mPersonId,
			long measurementConceptId,
			String measurementDate,
			String measurementDateTime,
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
		measurement.add("measurement_datetime", (measurementDateTime != null ? measurementDateTime : ""));
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
	private long addToDeviceExposure(
			long dePersonId,
			long deviceConceptId,
			String deviceExposureStartDate,
			String deviceExposureEndDate,
			long deviceTypeConceptId,
			String uniqueDeviceId,
			Long quantity,
			Long deProviderId,
			Long deVisitOccurrenceId,
			String deviceSourceValue,
			Long deviceSourceConceptId)
	{
		Row deviceExposure = new Row();
		long retId = deviceExposureId;
		deviceExposure.add("device_exposure_id", deviceExposureId++);
		deviceExposure.add("person_id", dePersonId);
		deviceExposure.add("device_concept_id", deviceConceptId); 
		deviceExposure.add("device_exposure_start_date", deviceExposureStartDate != null ? deviceExposureStartDate : "");
		deviceExposure.add("device_exposure_end_date", deviceExposureEndDate != null ? deviceExposureEndDate : "");
		deviceExposure.add("device_type_concept_id", deviceTypeConceptId); 
		deviceExposure.add("unique_device_id", uniqueDeviceId != null ? uniqueDeviceId : "");
		deviceExposure.add("quantity", (quantity != null ? quantity.toString() : ""));
		deviceExposure.add("provider_id", (deProviderId != null ? deProviderId.toString() : ""));
		deviceExposure.add("visit_occurrence_id", (deVisitOccurrenceId != null ? deVisitOccurrenceId.toString() : ""));
		deviceExposure.add("device_source_value", deviceSourceValue != null ? deviceSourceValue : "");
		deviceExposure.add("device_source_concept_id", (deviceSourceConceptId != null ? deviceSourceConceptId.toString() : ""));
		tableToRows.put("device_exposure", deviceExposure);
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
		conditionOccurrence.add("condition_status_source_value", "");
		conditionOccurrence.add("condition_status_concept_id", "");
		tableToRows.put("condition_occurrence", conditionOccurrence);
		return retId;
	}

	//********************  ********************//
	private long addToProcedureOccurrence(
			long poPersonId,
			long procedureConceptId,
			String procedureDate,
			String procedureDateTime,
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
		procedureOccurrence.add("procedure_datetime", procedureDateTime != null ? procedureDateTime : "");
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
			String procedureDateTime,
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
		procedureOccurrence.add("procedure_datetime", procedureDateTime != null ? procedureDateTime : "");
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
	private void addToVisitOccurrence(
			long inVisitOccurrenceId,
			String visitStartDate, 
			String visitEndDate,
			long visitConceptId, 
			Long visitTypeConceptId, 
			Long visitCareSiteId, 
			Long visitProviderId, 
			String visitSourceValue) {
		if (visitOccurrenceId < inVisitOccurrenceId)
			visitOccurrenceId = inVisitOccurrenceId;
		Row visitOccurrence = new Row();
		visitOccurrence.add("visit_occurrence_id", inVisitOccurrenceId);
		visitOccurrence.add("person_id", personId);
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
	}
	//********************  ********************//
	private long addToVisitOccurrence(
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
		visitOccurrence.add("person_id", personId);
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
	private static String replaceSpecialChars( String src )
	{
		src = src.replace("&amp;", "&");
		src = src.replace("&lt;", "<");
		src = src.replace("&gt;", ">");
		src = src.replace("&le;", "<=");
		src = src.replace("&ge;", ">=");

		return src;
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
