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
package org.ohdsi.jCdmBuilder.etls.switchbox;

import java.io.File;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
//import java.time.LocalDate;
//import java.time.Period;
//import java.util.ArrayList;
//import java.util.ArrayList;
//import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.ohdsi.databases.DbType;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.jCdmBuilder.DbSettings;
import org.ohdsi.jCdmBuilder.EtlReport;
//import org.ohdsi.jCdmBuilder.ObjectExchange;
import org.ohdsi.jCdmBuilder.cdm.CdmV5NullableChecker;
import org.ohdsi.jCdmBuilder.etls.switchbox.PersonVisitMap;
import org.ohdsi.jCdmBuilder.etls.switchbox.PersonVisitMap.PersonDomainData;
import org.ohdsi.jCdmBuilder.etls.switchbox.PersonVisitMap.TargetVisit;
//import org.ohdsi.jCdmBuilder.utilities.CSVFileChecker;
//import org.ohdsi.jCdmBuilder.utilities.CodeToConceptMap;
//import org.ohdsi.jCdmBuilder.utilities.CodeToDomainConceptMap;
//import org.ohdsi.jCdmBuilder.utilities.ETLUtils;
import org.ohdsi.jCdmBuilder.utilities.QCSampleConstructor;
//import org.ohdsi.jCdmBuilder.utilities.CodeToDomainConceptMap.CodeDomainData;
//import org.ohdsi.jCdmBuilder.utilities.CodeToDomainConceptMap.TargetConcept;
//import org.ohdsi.sql.StringUtils;
//import org.ohdsi.jCdmBuilder.utilities.CodeToDomainConceptMap.CodeDomainData;
//import org.ohdsi.jCdmBuilder.utilities.CodeToDomainConceptMap.TargetConcept;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.collections.OneToManyList;
//import org.ohdsi.utilities.collections.RowUtilities;
//import org.ohdsi.utilities.files.FileSorter;
import org.ohdsi.utilities.files.IniFile;
//import org.ohdsi.utilities.files.MultiRowIterator;
//import org.ohdsi.utilities.files.MultiRowIterator.MultiRowSet;
import org.ohdsi.utilities.files.ReadCSVFileWithHeader;
import org.ohdsi.utilities.files.Row;

public class SwitchboxETLtoV5 {

	public static int					MAX_ROWS_PER_PERSON				= 100000;
	public static long					BATCH_SIZE_DEFAULT				= 1000;
	public static boolean				GENERATE_QASAMPLES_DEFAULT		= false;
	public static double				QA_SAMPLE_PROBABILITY_DEFAULT	= 0.001;
	public enum Site {
		MAASTRICHT,
		ANTWERP
	}
	public static Site					SITE_DEFAULT					= Site.MAASTRICHT;
	public static String				SOURCE_FILE						= "SWITCHBOX.csv";
	public static long 					UNIT_PICOGRAM_PER_ML 			= 8845;
	public static long 					UNIT_SCORE 						= 44777566;
	public static long 					UNIT_SECONDS 					= 8555;
	public static long 					UNIT_SEE_TABLE 					= 8580;
	public static long 					UNIT_NA 						= 8497;
	public static long					UNIT_CM							= 8582;
	public static long					UNIT_KG							= 9529;
	public static long					UNIT_MMHG						= 8876;
	public static long					GENDER_MALE						= 8507;
	public static long					GENDER_FEMALE					= 8532;
	public static long					GENDER_UNKNOWN					= 8551;

	public static String[]				tablesInsideOP					= new String[] { "condition_occurrence", "procedure_occurrence", "drug_exposure", "death",
			"visit_occurrence", "observation", "measurement"					};
	public static String[]				fieldInTables					= new String[] { "condition_start_date", "procedure_date", "drug_exposure_start_date",
			"death_date", "visit_start_date", "observation_date", "measurement_date" };
	public static String[]				visitNumericFieldsToProcess		= new String[] { "AVLT_DELAYED", "AVLT_DELAYED_Z_SCORE", "AVLT_IMMEDIATE", "AVLT_IMMEDIATE_Z_SCORE", 
			"CATEGORY_FLUENCY_SUM", "FAQ", "GDS_SCORE", "MMSE_TOTAL", "PRIORITY_ATTENTION", "PRIORITY_ATTENTION_Z_SCORE", "PRIORITY_EXECUTIVE", "PRIORITY_EXECUTIVE_Z_SCORE", 
			"PRIORITY_LANGUAGE", "PRIORITY_MEMORY_DELAYED", "PRIORITY_MEMORY_DELAYED_Z_SCORE", "PRIORITY_MEMORY_IMMEDIATE", "PRIORITY_MEMORY_IMMEDIATE_Z_SCORE", 
			"TMTA", "TMTA_Z_SCORE", "TMTB", "TMTB_Z_SCORE" };
	public static String[]				visitStringFieldsToProcess		= new String[] { "GDS_30_ABNORMAL" };
	public static boolean				USE_VERBOSE_SOURCE_VALUES		= false;

//	private static String[] 			visits 							= new String[] { "_BASELINE", "_MONTH06", "_MONTH12", "_MONTH18", "_MONTH24", "_MONTH30", "_MONTH36" };  //, "_LAST" };
//	private static String[] 			normalVisits 					= new String[] { "_MONTH06", "_MONTH12", "_MONTH18", "_MONTH24", "_MONTH30", "_MONTH36" };

	private String						folder;
	private RichConnection				connection;
	private Site						site;
	private long						rowCount;
	private long						personId;
	private long						maxPersonId;
	private long						observationPeriodId;
//	private long						drugExposureId;
	private long						visitOccurrenceId;
	private long						careSiteId;
//	private long						observationId;
//	private long						measurementId;
	private long						factId;
	private long						factAttributeId;

	private PersonVisitMap				personVisits;
	private OneToManyList<String, Row>	tableToRows;
	private Map<String, Long>			baseFieldToConcept				= new HashMap<String, Long>();
	private Map<String, Long>			baseFieldToUnit					= new HashMap<String, Long>();
	private Map<String, Long>			diagnosisCodeToConcept			= new HashMap<String, Long>();
//	private Map<String, Long>			sourceToCareSiteId				= new HashMap<String, Long>();
	private Map<String, Long>			personRefToId					= new HashMap<String, Long>();
	private Map<Long, Long>				personIdToGenderConceptId		= new HashMap<Long, Long>();
	private Map<Long, Integer>			personIdToAge					= new HashMap<Long, Integer>();
	private Map<String, String>			keyToFieldName					= new HashMap<String, String>();
	private Map<String, Double>			keyToCutoff						= new HashMap<String, Double>();

	private QCSampleConstructor			qcSampleConstructor;
	private EtlReport					etlReport;
	private CdmV5NullableChecker		cdmv5NullableChecker			= new CdmV5NullableChecker();

	private IniFile						settings;
	private long						batchSize;				// How many patient records, and all related records, should be loaded before inserting batch in target
	public Boolean						generateQaSamples;		// Generate QA Samples at end of ETL
	public double						qaSampleProbability;	// The sample probability value used to include a patient's records in the QA Sample export
	
	// ******************** Process ********************//
	public void process(String folder, DbSettings dbSettings, int maxPersons, int versionId, boolean newCdmStructure) {
		this.setFolder(folder);
		loadSettings();
		connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		connection.setContext(this.getClass());
		connection.use(dbSettings.database);
		List<String> tableNames = connection.getTableNames(dbSettings.database);

		if ((newCdmStructure) || (!tableNames.contains("sb_fact")))
			createCustomTablesAndConcepts(dbSettings);

		if (!tableNames.contains("cost"))
			createCostTable(dbSettings);
		if (!tableNames.contains("_version"))
			connection.execute("CREATE TABLE _version(version_id INT,version_date DATE);");	
		
		truncateTables(connection);

		String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
		connection.execute("INSERT INTO _version (version_id, version_date) VALUES (" + versionId + ", '" + date + "')");

		rowCount = 0;
		personId = 0;
		maxPersonId = 0;
		observationPeriodId = 0;
//		drugExposureId = 0;
		visitOccurrenceId = 0;
//		observationId = 0;
//		measurementId = 0;
		factId = 0;
		factAttributeId = 0;
		careSiteId = 0;
		baseFieldToConcept.clear();
		baseFieldToUnit.clear();
		diagnosisCodeToConcept.clear();
		personRefToId.clear();
		personVisits = new PersonVisitMap("Person visits");
		
		loadMappings(dbSettings);

		if (generateQaSamples)
			qcSampleConstructor = new QCSampleConstructor(folder + "/sample", qaSampleProbability);
		tableToRows = new OneToManyList<String, Row>();
		etlReport = new EtlReport(folder);

		StringUtilities.outputWithTime("Populating CDM_Source table");
		populateCdmSourceTable();

		addCareSiteRecord();  // Add default care_site record for current site
		
		StringUtilities.outputWithTime("Processing rows");

		for (Row row : new ReadCSVFileWithHeader(folder + "/" + SOURCE_FILE)) {
			row.upperCaseFieldNames();
			processSourceRecord(row);

			if (rowCount == maxPersons) {
				System.out.println("Reached limit of " + maxPersons + " rows, terminating");
				break;
			}
			if (rowCount % batchSize == 0) {
				insertBatch();
				System.out.println("Processed " + rowCount + " rows");
			}
		}

		System.out.println("Generating observation period records");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		for (Long personRefId : personRefToId.values()) {
			PersonDomainData data = personVisits.getPersonData(personRefId);
			int idx = 0;
			String minDate = null;
			String maxDate = null;
			for (TargetVisit targetVisit : data.targetVisits) {
				if (idx == 0) {
					minDate = targetVisit.visitDate;
					maxDate = targetVisit.visitDate;
				} else {
					try {
						if (sdf.parse(targetVisit.visitDate).before(sdf.parse(minDate)))
							minDate = targetVisit.visitDate;
						if (sdf.parse(maxDate).before(sdf.parse(targetVisit.visitDate)))
							maxDate = targetVisit.visitDate;
					} catch (ParseException e) {
						//					isDate = false;
					}
				}
				idx++;
			}
			if (minDate != null) 
				addToObservationPeriod(personRefId, minDate, maxDate);
		}
		
		removeRowsOutsideOfObservationTime();
		
		insertBatch();

		System.out.println("Processed " + rowCount + " rows");
		if (generateQaSamples)
			qcSampleConstructor.addCdmData(connection, dbSettings.database);

		//		String etlReportName = etlReport.generateETLReport(icd9ProcToConcept, icd9ToConcept, atcToConcept);
		//		System.out.println("An ETL report was generated and written to :" + etlReportName);
		//
		//		String etlReportName2 = etlReport.generateETLReport(specialtyToConcept);
		//		System.out.println("An ETL report was generated and written to :" + etlReportName2);
		//
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
				"procedure_occurrence", "provider", "specimen", "visit_occurrence", "sb_fact", "sb_fact_attribute" };
		for (String table : tables)
			targetConnection.execute("TRUNCATE TABLE " + table);	}

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
	private void loadMappings(DbSettings dbSettings) {
		StringUtilities.outputWithTime("Loading mappings from server");
		RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		connection.setContext(this.getClass());
		connection.use(dbSettings.database);

		System.out.println("- Loading base field mapping");
		for (Row row : new ReadCSVFileWithHeader(this.getClass().getResourceAsStream("csv/visit_base_fields.csv"))) {
			row.upperCaseFieldNames();
			baseFieldToConcept.put(row.get("BASE_FIELD"), row.getLong("CONCEPT_ID"));
			baseFieldToUnit.put(row.get("BASE_FIELD"), row.getLong("UNIT_CONCEPT_ID"));
		}

		System.out.println("- Loading field mapping");
		for (Row row : new ReadCSVFileWithHeader(this.getClass().getResourceAsStream("csv/field_mappings.csv"))) {
			row.upperCaseFieldNames();
			String tmpS = null;
			switch (site) {
			case MAASTRICHT:
				tmpS = row.get("SITE1_FIELD");
				break;
			case ANTWERP:
				tmpS = row.get("SITE2_FIELD");
				break;
			}
			if ((tmpS != null) && (!tmpS.equals("")))
				keyToFieldName.put(row.get("KEY"), tmpS);
			else
				keyToFieldName.put(row.get("KEY"), null);
		}

		System.out.println("- Loading cut-off values");
		for (Row row : new ReadCSVFileWithHeader(this.getClass().getResourceAsStream("csv/cut_off_values.csv"))) {
			row.upperCaseFieldNames();
			String tmpS = null;
			switch (site) {
			case MAASTRICHT:
				tmpS = row.get("SITE1_CUTOFF");
				break;
			case ANTWERP:
				tmpS = row.get("SITE2_CUTOFF");
				break;
			}
			if ((tmpS != null) && (!tmpS.equals("")))
				keyToCutoff.put(row.get("KEY"), Double.valueOf(tmpS));
			else
				keyToCutoff.put(row.get("KEY"), null);
		}

		System.out.println("- Loading diagnosis code mapping");
		for (Row row : new ReadCSVFileWithHeader(this.getClass().getResourceAsStream("csv/diagnosis.csv"))) {
			row.upperCaseFieldNames();
			diagnosisCodeToConcept.put(row.get("KEY"), row.getLong("CONCEPT_ID"));
		}

		StringUtilities.outputWithTime("Finished loading mappings");
	}

	//********************  ********************//
	private void loadSettings() {

		batchSize = BATCH_SIZE_DEFAULT;
		generateQaSamples = GENERATE_QASAMPLES_DEFAULT;
		qaSampleProbability = QA_SAMPLE_PROBABILITY_DEFAULT;
		site = SITE_DEFAULT;

		File f = new File("Switchbox.ini");
		if(f.exists() && !f.isDirectory()) { 
			try {
				settings = new IniFile("Switchbox.ini");
				try {
					Long numParam = Long.valueOf(settings.get("batchSize"));
					if (numParam != null)
						batchSize = numParam;
				} catch (Exception e) {
					batchSize = BATCH_SIZE_DEFAULT;
				}
			} catch (Exception e) {
				StringUtilities.outputWithTime("No .ini file found");
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
				Integer tmpInt = null;
				String strParam = settings.get("site");
				if ((strParam != null) && (!strParam.equals(""))) {
					tmpInt = Integer.valueOf(strParam);
					if (tmpInt == 2)
						site = Site.ANTWERP;
					else
						site = Site.MAASTRICHT;
				}
				else
					site = SITE_DEFAULT;
			} catch (Exception e) {
				site = SITE_DEFAULT;
			}

		}
	}

	//******************** processSourceRecord ********************//
	@SuppressWarnings("unused")
	private void processSourceRecord(Row row) {
		String tmpS = null;
//		Double fValue = null;
		Long conceptId = null;
//		Long unitConceptId = null;
		long visitFactType;

		etlReport.registerIncomingData("SWITCHBOX", row);

		rowCount++;
		String pid = getRowValue(row,"PERSON_ID");
		Long refPersonId = personRefToId.get(pid);
		if (refPersonId == null)
			refPersonId = addToPerson(row);
		personId = refPersonId;

		if (generateQaSamples)
			qcSampleConstructor.registerPersonData("SWITCHBOX", row, personId); //row.getLong("SUBJECT_ID"));

		Long visitCareSiteId = careSiteId; // sourceToCareSiteId.get(row.get("CENTER").trim().toLowerCase());
		String visit = null;
		String visitNumber = getRowValue(row, "VISIT_NUMBER");
		if (visitNumber != null) {
			switch (visitNumber) {
			case "1":
				visit = "_BASELINE";
				visitFactType = 2000000260;
				break;
			case "2":
				visit = "_MONTH06";
				visitFactType = 2000000261;
				break;
			case "3":
				visit = "_MONTH12";
				visitFactType = 2000000262;
				break;
			case "4":
				visit = "_MONTH18";
				visitFactType = 2000000263;
				break;
			case "5":
				visit = "_MONTH24";
				visitFactType = 2000000264;
				break;
			case "6":
				visit = "_MONTH30";
				visitFactType = 2000000270;
				break;
			case "7":
				visit = "_MONTH36";
				visitFactType = 2000000271;
				break;
			default:
				visitFactType = 0;
				break;
			}

			if (visit != null) {
				String visitDate = null;
				Long visitYear = getLongRowValue(row, "VISIT_DATE_YEAR");
				Long visitMonth = getLongRowValue(row, "VISIT_DATE_MONTH");
				Long visitDay = getLongRowValue(row, "VISIT_DATE_DAY");
				if ((visitYear != null) && (visitMonth != null) && (visitDay != null)) {
					if ((visitYear > 1900) && (visitMonth > 0) && (visitMonth < 13) && (visitDay > 0) && (visitDay < 32)) {
						visitDate = String.format("%04d", visitYear)+"-"+String.format("%02d", visitMonth)+"-"+String.format("%02d", visitDay);
					}
				}

				if (isADateString(visitDate)) {
					visitOccurrenceId++;
					addToVisitOccurrence(row, visit, visitDate, visitCareSiteId, visitNumber);
					Double yearsEducation = null;
					Long educationLevel = null;
					Double eduLow = null;
					Double eduHigh = null;
					Double amyloidBeta142 = null;
					Double totalTau = null;
					Double phosphorylatedTau = null;
					Double cdrTotal = null;
					Double avltDelayed = null;
					Double avltDelayed_Z_score = null;
					Double avltImmediate = null;
					Double avltImmediate_Z_score = null;
					Double gdsScore = null;
					Double hdsScore = null;
					Double mmseTotal = null;
					Double tmtA = null;
					Double tmtA_Z_score = null;
					Double tmtB = null;
					Double tmtB_Z_score = null;
					Double tmtC = null;
					Double stroop1 = null;
					Double stroop1_Z_score = null;
					Double stroop2 = null;
					Double stroop2_Z_score = null;
					Double stroop3 = null;
					Double stroop3_Z_score = null;
					Double digSpanForward = null;
					Double digSpanBackward = null;
					Double storyImmediate = null;
					Double storyImmediate_Z_score = null;
					Double storyDelayed = null;
					Double storyDelayed_Z_score = null;
					Long vatConceptId = null;
					Double vatScore = null;
					Double animalFluency1min = null;
					Double npiQTotal = null;
					Double iadlTotal = null;
					String diagnosisCode = null;
					
					// ========== Education ==========
					String tmpY = getRowValue(row, "YEARS_EDUCATION");
					if ((tmpY != null) && (!tmpY.equals(""))) {
						yearsEducation = Double.valueOf(tmpY);
						addToFact(personId,(long) 2000000095, visitDate, visitFactType, null, yearsEducation, null, null, null,  // Education level: 2000000095
								(long) 9448, null, visitOccurrenceId,"",getField("YEARS_EDUCATION"), null, null, tmpS); // Year: 9448
						if (yearsEducation < 7) {
							eduLow = 1.0;
							eduHigh = 0.0;
						} else if (yearsEducation > 6) {
							eduLow = 0.0;
							eduHigh = 0.0;
							if (yearsEducation > 12) {
								eduHigh = 1.0;
							}
						}
					}

					// ========== Facts (values) ==========
					tmpS = getRowValue(row, "AMYLOID_BETA_1_42");  //  521.92
					if ((tmpS != null) && (StringUtilities.isNumber(tmpS))) {
						amyloidBeta142 = Double.valueOf(tmpS);
						addToFact(personId,(long) 2000000070, visitDate, visitFactType, null, amyloidBeta142, null, null, null,  // CSF amyloid beta 1-42: 2000000070
								UNIT_PICOGRAM_PER_ML, null, visitOccurrenceId,"",getField("AMYLOID_BETA_1_42"), null, null, tmpS); 
					}
					tmpS = getRowValue(row, "TOTAL_TAU"); // 40.55
					if ((tmpS != null) && (StringUtilities.isNumber(tmpS))) {
						totalTau = Double.valueOf(tmpS);
						addToFact(personId,(long) 2000000075, visitDate, visitFactType, null, totalTau, null, null, null,  // CSF total tau: 2000000075
								UNIT_PICOGRAM_PER_ML, null, visitOccurrenceId,"",getField("TOTAL_TAU"), null, null, tmpS); 
					}
					tmpS = getRowValue(row, "PHOSPHORYLATED_TAU");  //   72.33
					if ((tmpS != null) && (StringUtilities.isNumber(tmpS))) {
						phosphorylatedTau = Double.valueOf(tmpS);
						addToFact(personId,(long) 2000000073, visitDate, visitFactType, null, phosphorylatedTau, null, null, null,  // CSF phosphorylated tau: 2000000073
								UNIT_PICOGRAM_PER_ML, null, visitOccurrenceId,"",getField("PHOSPHORYLATED_TAU"), null, null, tmpS); 
					}
					tmpS = getRowValue(row, "CDR_TOTAL");  
					if ((tmpS != null) && (StringUtilities.isNumber(tmpS))) {
						cdrTotal = Double.valueOf(tmpS);
						addToFact(personId,(long) 2000000044, visitDate, visitFactType, null, cdrTotal, null, null, null,  // Clinical dementia rating total: 2000000044
								UNIT_SCORE, null, visitOccurrenceId,"",getField("CDR_TOTAL"), null, null, tmpS); 
					}
					tmpS = getRowValue(row, "AVLT_DELAYED");  
					if ((tmpS != null) && (StringUtilities.isNumber(tmpS))) {
						avltDelayed = Double.valueOf(tmpS);
						addToFact(personId,(long) 2000000015, visitDate, visitFactType, null, avltDelayed, null, null, null,  // AVLT delayed: 2000000015
								UNIT_SCORE, null, visitOccurrenceId,"",getField("AVLT_DELAYED"), null, null, tmpS); 
					}
					tmpS = getRowValue(row, "AVLT_IMMEDIATE");  
					if ((tmpS != null) && (StringUtilities.isNumber(tmpS))) {
						avltImmediate = Double.valueOf(tmpS);
						addToFact(personId,(long) 2000000017, visitDate, visitFactType, null, avltImmediate, null, null, null,  // AVLT immediate: 2000000017
								UNIT_SCORE, null, visitOccurrenceId,"",getField("AVLT_IMMEDIATE"), null, null, tmpS); 
					}
					tmpS = getRowValue(row, "GDS_SCORE"); 
					if ((tmpS != null) && (StringUtilities.isNumber(tmpS))) {
						gdsScore = Double.valueOf(tmpS);
						addToFact(personId,(long) 2000000111, visitDate, visitFactType, null, gdsScore, null, null, null,  // Geriatric Depression Scale - 15 question version: 2000000111
								UNIT_SCORE, null, visitOccurrenceId,"",getField("GDS_SCORE"), null, null, tmpS); 
					}
					tmpS = getRowValue(row, "HDS"); 
					if ((tmpS != null) && (StringUtilities.isNumber(tmpS))) {
						hdsScore = Double.valueOf(tmpS);
						addToFact(personId,(long) 2000000121, visitDate, visitFactType, null, hdsScore, null, null, null,  // Hamilton Depression Scale: 2000000121
								UNIT_SCORE, null, visitOccurrenceId,"",getField("HDS"), null, null, tmpS); 
					}
					tmpS = getRowValue(row, "MMSE_TOTAL"); 
					if ((tmpS != null) && (StringUtilities.isNumber(tmpS))) {
						mmseTotal = Double.valueOf(tmpS);
						addToFact(personId,(long) 2000000166, visitDate, visitFactType, null, mmseTotal, null, null, null,  // Mini mental state exam total score: 2000000166
								UNIT_SCORE, null, visitOccurrenceId,"",getField("MMSE_TOTAL"), null, null, tmpS); 
					}
					tmpS = getRowValue(row, "TMTA"); 
					if ((tmpS != null) && (StringUtilities.isNumber(tmpS))) {
						tmtA = Double.valueOf(tmpS);
						addToFact(personId,(long) 2000000210, visitDate, visitFactType, null, tmtA, null, null, null,  // Trail Making Test - Time (in seconds) to complete Trails A.: 2000000210
								UNIT_SECONDS, null, visitOccurrenceId,"",getField("TMTA"), null, null, tmpS); 
					}
					tmpS = getRowValue(row, "TMTB");
					if ((tmpS != null) && (StringUtilities.isNumber(tmpS))) {
						tmtB = Double.valueOf(tmpS);
						addToFact(personId,(long) 2000000212, visitDate, visitFactType, null, tmtB, null, null, null,  // Trail Making Test - Time (in seconds) to complete Trails B.: 2000000212
								UNIT_SECONDS, null, visitOccurrenceId,"",getField("TMTB"), null, null, tmpS); 
					}
					tmpS = getRowValue(row, "TMTC");
					if ((tmpS != null) && (StringUtilities.isNumber(tmpS))) {
						tmtC = Double.valueOf(tmpS);
						addToFact(personId,(long) 2000000274, visitDate, visitFactType, null, tmtC, null, null, null,  // Trail Making Test - Time (in seconds) to complete Trails C.: 2000000274
								UNIT_SECONDS, null, visitOccurrenceId,"",getField("TMTC"), null, null, tmpS); 
					}
					tmpS = getRowValue(row, "STROOP1");
					if ((tmpS != null) && (StringUtilities.isNumber(tmpS))) {
						stroop1 = Double.valueOf(tmpS);
						addToFact(personId,(long) 2000000276, visitDate, visitFactType, null, stroop1, null, null, null,  // Stroop Part 1: 2000000276
								UNIT_SECONDS, null, visitOccurrenceId,"",getField("STROOP1"), null, null, tmpS); 
					}
					tmpS = getRowValue(row, "STROOP2");
					if ((tmpS != null) && (StringUtilities.isNumber(tmpS))) {
						stroop2 = Double.valueOf(tmpS);
						addToFact(personId,(long) 2000000278, visitDate, visitFactType, null, stroop2, null, null, null,  // Stroop Part 2: 2000000278
								UNIT_SECONDS, null, visitOccurrenceId,"",getField("STROOP2"), null, null, tmpS); 
					}
					tmpS = getRowValue(row, "STROOP3");
					if ((tmpS != null) && (StringUtilities.isNumber(tmpS))) {
						stroop3 = Double.valueOf(tmpS);
						addToFact(personId,(long) 2000000280, visitDate, visitFactType, null, stroop3, null, null, null,  // Stroop Part 3: 2000000280
								UNIT_SECONDS, null, visitOccurrenceId,"",getField("STROOP3"), null, null, tmpS); 
					}
					tmpS = getRowValue(row, "DIG_SPAN_FORWARD");
					if ((tmpS != null) && (StringUtilities.isNumber(tmpS))) {
						digSpanForward = Double.valueOf(tmpS);
						addToFact(personId,(long) 2000000084, visitDate, visitFactType, null, digSpanForward, null, null, null,  // Digit span forward: 200000084
								UNIT_SCORE, null, visitOccurrenceId,"",getField("DIG_SPAN_FORWARD"), null, null, tmpS); 
					}
					tmpS = getRowValue(row, "DIG_SPAN_BACKWARD");
					if ((tmpS != null) && (StringUtilities.isNumber(tmpS))) {
						digSpanBackward = Double.valueOf(tmpS);
						addToFact(personId,(long) 2000000082, visitDate, visitFactType, null, digSpanBackward, null, null, null,  // Digit span backward: 2000000082
								UNIT_SCORE, null, visitOccurrenceId,"",getField("DIG_SPAN_BACKWARD"), null, null, tmpS); 
					}
					tmpS = getRowValue(row, "STORY_IMMEDIATE");
					if ((tmpS != null) && (StringUtilities.isNumber(tmpS))) {
						storyImmediate = Double.valueOf(tmpS);
						addToFact(personId,(long) 2000000208, visitDate, visitFactType, null, storyImmediate, null, null, null,  // Story immediate: 2000000208
								UNIT_SCORE, null, visitOccurrenceId,"",getField("STORY_IMMEDIATE"), null, null, tmpS); 
					}
					tmpS = getRowValue(row, "STORY_DELAYED");
					if ((tmpS != null) && (StringUtilities.isNumber(tmpS))) {
						storyDelayed = Double.valueOf(tmpS);
						addToFact(personId,(long) 2000000206, visitDate, visitFactType, null, storyDelayed, null, null, null,  // Story delayed: 2000000206
								UNIT_SCORE, null, visitOccurrenceId,"",getField("STORY_DELAYED"), null, null, tmpS); 
					}
					tmpS = getRowValue(row, "VAT_VERS");
					if ((tmpS != null) && (tmpS.length() > 0)) {
						switch (tmpS.toLowerCase()) {
						case "kort":
							vatConceptId = (long) 2000000215; // VAT-12
							break;
						case "lang":
							vatConceptId = (long) 2000000216; // VAT-24
							break;
						}
						if (vatConceptId != null) {
							tmpS = getRowValue(row, "VAT_TOT");
							if ((tmpS != null) && (StringUtilities.isNumber(tmpS))) {
								vatScore = Double.valueOf(tmpS);
								addToFact(personId,vatConceptId, visitDate, visitFactType, null, vatScore, null, null, null,  // VAT-12: 2000000215, or VAT-24: 2000000216
										UNIT_SCORE, null, visitOccurrenceId,"",getField("VAT_TOT"), null, null, tmpS); 
							}		
						}
					}
					for (int i = 0; i < 2; i++) {
						String vMode = getRowValue(row, "VRBL_FLUENCY_MODE_" + Integer.toString(i+1));
						if (((vMode != null) && (vMode.toUpperCase().equals("DIEREN")))) {
							tmpS = getRowValue(row, "VRBL_FLUENCY_SCORE_" + Integer.toString(i+1));
							if ((tmpS != null) && (StringUtilities.isNumber(tmpS))) {
								animalFluency1min = Double.valueOf(tmpS);
								addToFact(personId,(long) 2000000009, visitDate, visitFactType, null, animalFluency1min, null, null, null,  // Animals fluency 1 min: 2000000009
										UNIT_SCORE, null, visitOccurrenceId,"",getField("VRBL_FLUENCY_SCORE_" + Integer.toString(i+1)), null, null, tmpS); 
							}							
						}
					}
					tmpS = getRowValue(row, "NPI_Q_TOTAL");
					if ((tmpS != null) && (StringUtilities.isNumber(tmpS))) {
						npiQTotal = Double.valueOf(tmpS);
						addToFact(personId,(long) 2000000174, visitDate, visitFactType, null, npiQTotal, null, null, null,  // NPI-Q total: 2000000174
								UNIT_SCORE, null, visitOccurrenceId,"",getField("NPI_Q_TOTAL"), null, null, tmpS); 
					}
					tmpS = getRowValue(row, "IADL_TOTAL");
					if ((tmpS != null) && (StringUtilities.isNumber(tmpS))) {
						iadlTotal = Double.valueOf(tmpS);
						addToFact(personId,(long) 2000000138, visitDate, visitFactType, null, iadlTotal, null, null, null,  // Instrumental Activities of Daily Living: 2000000138
								UNIT_SCORE, null, visitOccurrenceId,"",getField("IADL_TOTAL"), null, null, tmpS); 
					}
					if ((amyloidBeta142 != null) && (keyToCutoff.get("amyloid_beta_1_42_cutoff") != null)) {
						if (amyloidBeta142 < keyToCutoff.get("amyloid_beta_1_42_cutoff"))
							conceptId = (long) 2000000238; // Yes
						else
							conceptId = (long) 2000000239; // No
						addToFact(personId,(long) 2000000071, visitDate, visitFactType, null, null, null, null, conceptId,  // CSF amyloid beta 1-42 abnormality: 2000000071
								UNIT_NA, null, visitOccurrenceId,"",getField("AMYLOID_BETA_1_42"), null, null, Double.toString(amyloidBeta142)); 
					}
					if ((totalTau != null) && (keyToCutoff.get("total_tau_cutoff") != null)) {
						if (totalTau > keyToCutoff.get("total_tau_cutoff"))
							conceptId = (long) 2000000238; // Yes
						else
							conceptId = (long) 2000000239; // No
						addToFact(personId,(long) 2000000076, visitDate, visitFactType, null, null, null, null, conceptId,  // CSF total tau abnormality: 2000000076
								UNIT_NA, null, visitOccurrenceId,"",getField("TOTAL_TAU"), null, null, Double.toString(totalTau)); 
					}
					if ((phosphorylatedTau != null) && (keyToCutoff.get("phosphorylated_tau_cutoff") != null)) {
						if (phosphorylatedTau > keyToCutoff.get("phosphorylated_tau_cutoff"))
							conceptId = (long) 2000000238; // Yes
						else
							conceptId = (long) 2000000239; // No
						addToFact(personId,(long) 2000000074, visitDate, visitFactType, null, null, null, null, conceptId,  // CSF phosphorylated tau abnormality: 2000000074
								UNIT_NA, null, visitOccurrenceId,"",getField("PHOSPHORYLATED_TAU"), null, null, Double.toString(phosphorylatedTau)); 
					}
					if ((gdsScore != null) && (keyToCutoff.get("gds_30_cutoff") != null)) {
						if (gdsScore > keyToCutoff.get("gds_30_cutoff"))
							conceptId = (long) 2000000238; // Yes
						else
							conceptId = (long) 2000000239; // No
						addToFact(personId,(long) 2000000112, visitDate, visitFactType, null, null, null, null, conceptId,  // Geriatric Depression Scale - 15 question version abnormality: 2000000112
								UNIT_NA, null, visitOccurrenceId,"",getField("GDS_SCORE"), null, null, Double.toString(gdsScore)); 
					}	
					
					// ========== Calculations ==========
					Long genderConceptId = personIdToGenderConceptId.get(personId);
					Integer age = personIdToAge.get(personId);
					if ((yearsEducation != null) && (genderConceptId != null) && (age != null)) {
						if (tmtA != null) {
							if (genderConceptId == GENDER_FEMALE) {
								if (yearsEducation < 13) {
									if (age > 50)		tmtA_Z_score = (44.4 - tmtA) / 13.7;
									else if (age >=70)	tmtA_Z_score = (48.1 - tmtA) / 15.3;
									else if (age >=80)	tmtA_Z_score = (53.6 - tmtA) / 20.4;
								}
								else if (yearsEducation > 12) {
									if (age > 50)		tmtA_Z_score = (39.7 - tmtA) / 11.0;
									else if (age >=70)	tmtA_Z_score = (51.0 - tmtA) / 22.3;
									else if (age >=80)	tmtA_Z_score = (61.0 - tmtA) / 20.4;
								}
							}
							else if (genderConceptId == GENDER_MALE) { // male
								if (yearsEducation < 13) {
									if (age > 50)		tmtA_Z_score = (44.6 - tmtA) / 13.7;
									else if (age >=70)	tmtA_Z_score = (50.1 - tmtA) / 16.2;
									else if (age >=80)	tmtA_Z_score = (62.0 - tmtA) / 22.1;
								}
								else if (yearsEducation > 12) {
									if (age > 50)		tmtA_Z_score = (43.8 - tmtA) / 17.2;
									else if (age >=70)	tmtA_Z_score = (45.8 - tmtA) / 15.1;
									else if (age >=80)	tmtA_Z_score = (59.6 - tmtA) / 12.2;
								}
							}
							if (tmtA_Z_score != null) {
								addToFact(personId,(long) 2000000211, visitDate, visitFactType, null, tmtA_Z_score, null, null, null,  // Trail Making Test - section A z-score.: 2000000211
										UNIT_NA, null, visitOccurrenceId,"",getField("TMTA"), null, null, tmtA.toString()); 
								
							}
						}
						
						if (tmtB != null) {
							if (genderConceptId == GENDER_FEMALE) {
								if (yearsEducation < 13) {
									if (age > 50)		tmtB_Z_score = (106.6 - tmtB) / 37.9;
									else if (age >=70)	tmtB_Z_score = (138.3 - tmtB) / 58.9;
									else if (age >=80)	tmtB_Z_score = (141.6 - tmtB) / 42.5;
								}
								else if (yearsEducation > 12) {
									if (age > 50)		tmtB_Z_score = (95.1 - tmtB) / 35.9;
									else if (age >=70)	tmtB_Z_score = (107.6 - tmtB) / 40.0;
									else if (age >=80)	tmtB_Z_score = (145.0 - tmtB) / 42.5;
								}
							}
							else if (genderConceptId == GENDER_MALE) { // male
								if (yearsEducation < 13) {
									if (age > 50)		tmtB_Z_score = (117.8 - tmtB) / 50.9;
									else if (age >=70)	tmtB_Z_score = (141.0 - tmtB) / 65.6;
									else if (age >=80)	tmtB_Z_score = (190.7 - tmtB) / 69.9;
								}
								else if (yearsEducation > 12) {
									if (age > 50)		tmtB_Z_score = (96.9 - tmtB) / 34.2;
									else if (age >=70)	tmtB_Z_score = (104.7 - tmtB) / 31.7;
									else if (age >=80)	tmtB_Z_score = (124.6 - tmtB) / 34.2;
								}
							}
							if (tmtB_Z_score != null) {
								addToFact(personId,(long) 2000000213, visitDate, visitFactType, null, tmtB_Z_score, null, null, null,  // Trail Making Test - section B z-score.: 2000000213
										UNIT_NA, null, visitOccurrenceId,"",getField("TMTB"), null, null, tmtB.toString()); 
							}
						}
					}
					
					Double tmpGender = null;
					if ((genderConceptId != null) && (genderConceptId != GENDER_UNKNOWN)) {
						if (genderConceptId == GENDER_MALE)
							tmpGender = 0.0;
						else if (genderConceptId == GENDER_FEMALE)
							tmpGender = 1.0;
					}

					// ===== Stroop Z scores =====
					// Stroop 1_type1_MST Z-score = ((1/Stroop 1)-(0.01566 + 0.000315*Age + -0.00112*Edulow + 0.001465*Eduhigh + -0.0000032*(Age*Age)))/0.0034.
					// Stroop3 Z-score=((1/Stroop 3)-(0.001926 + 0.000348*age + 0.0002244*sex + -0.0006982*edulow + 0.001015*eduhigh + -0.000003522*age^2))/0.002
					if ((stroop1 != null) && (age != null) && (eduLow != null) && (eduHigh != null)) {
						stroop1_Z_score = ((1.0/stroop1)-(0.01566 + (0.000315*age) - (0.00112*eduLow) + (0.001465*eduHigh) - (0.0000032*(age*age)) ) ) / 0.0034;
						addToFact(personId,(long) 2000000277, visitDate, visitFactType, null, stroop1_Z_score, null, null, null,  // Stroop Part 1 - Z score: 2000000277
								UNIT_NA, null, visitOccurrenceId,"",getField("STROOP1"), null, null, stroop1.toString()); 
					}
					if ((stroop2 != null) && (age != null) && (tmpGender != null) && (eduLow != null) && (eduHigh != null)) {
						Double tmpStr2_predict = (52.468 + (0.209 * age) + (0.007 * age * age) + (2.390 * tmpGender) + (4.235 * eduLow) + (-2.346 * eduHigh));
						Double tmpStr2_sd = null;
						if (tmpStr2_predict <= 51.661)
							tmpStr2_sd = 7.988;
						else if ((tmpStr2_predict > 51.661) && (tmpStr2_predict <= 55.861))
							tmpStr2_sd = 8.459;
						else if ((tmpStr2_predict > 55.861) && (tmpStr2_predict <= 60.713))
							tmpStr2_sd = 9.419;
						else if (tmpStr2_predict > 60.713)
							tmpStr2_sd = 10.587;
						if (tmpStr2_sd != null) {
							stroop2_Z_score = - ((stroop2 - tmpStr2_predict) / tmpStr2_sd);
							addToFact(personId,(long) 2000000279, visitDate, visitFactType, null, stroop2_Z_score, null, null, null,  // Stroop Part 2 - Z score: 2000000279
									UNIT_NA, null, visitOccurrenceId,"",getField("STROOP2"), null, null, stroop2.toString()); 
						}
					}
					if ((stroop3 != null) && (age != null) && (tmpGender != null) && (eduLow != null) && (eduHigh != null)) {
						stroop3_Z_score = ((1.0/stroop3) - (0.001926 + (0.000348*age) + (0.0002244*tmpGender) - (0.0006982*eduLow) + (0.001015*eduHigh) - (0.000003522*(age*age)) )) / 0.002;
						addToFact(personId,(long) 2000000281, visitDate, visitFactType, null, stroop3_Z_score, null, null, null,  // Stroop Part 3 - Z score: 2000000281
								UNIT_NA, null, visitOccurrenceId,"",getField("STROOP3"), null, null, stroop3.toString()); 
					}
					
					// ===== AVLT Z scores =====
					// AVLTdelzscore = ( AVLTdel - (10.924 + (-0.073 * (age - 50)) + (-0.0009 * (age - 50)**2) + (-1.197 * sex) + (-0.844 * edulow) + (0.424 * eduhigh)))/2.496.
					// AVLTimmzscore = ( AVLTimm - (49.672 + (-0.247 * (age - 50)) + (-0.0033 * (age - 50)**2) + (-4.227 * sex) + (-3.055 * edulow) + (2.496 * eduhigh)))/7.826.
					if ((avltDelayed != null) && (age != null) && (tmpGender != null) && (eduLow != null) && (eduHigh != null)) {
						avltDelayed_Z_score = ( avltDelayed - (10.924 + (-0.073 * (age - 50.0)) + (-0.0009 * ((age - 50.0)*(age - 50.0))) + (-1.197 * tmpGender) + (-0.844 * eduLow) + (0.424 * eduHigh))) / 2.496;
						addToFact(personId,(long) 2000000016, visitDate, visitFactType, null, avltDelayed_Z_score, null, null, null,  // AVLT delayed z-score: 2000000016
								UNIT_NA, null, visitOccurrenceId,"",getField("AVLT_DELAYED"), null, null, avltDelayed.toString()); 
					}
					if ((avltImmediate != null) && (age != null) && (tmpGender != null) && (eduLow != null) && (eduHigh != null)) {
						avltImmediate_Z_score = ( avltImmediate - (49.672 + (-0.247 * (age - 50.0)) + (-0.0033 * ((age - 50.0)*(age - 50.0))) + (-4.227 * tmpGender) + (-3.055 * eduLow) + (2.496 * eduHigh)))/7.826;
						addToFact(personId,(long) 2000000018, visitDate, visitFactType, null, avltImmediate_Z_score, null, null, null,  // AVLT immediate z-score: 2000000018
								UNIT_NA, null, visitOccurrenceId,"",getField("AVLT_IMMEDIATE"), null, null, avltImmediate.toString()); 
					}
					
					// ===== Story Z scores =====	
					// Story immediate Z score
					if ((storyImmediate != null) && (age != null)) {
						if ((age >= 25) && (age < 35))
							storyImmediate_Z_score = (storyImmediate - 14.5) / 1.84;
						else if ((age >= 35) && (age < 45))
							storyImmediate_Z_score = (storyImmediate - 14.4) / 2.81;
						else if ((age >= 45) && (age < 55))
							storyImmediate_Z_score = (storyImmediate - 16.0) / 1.76;
						else if ((age >= 55) && (age < 65))
							storyImmediate_Z_score = (storyImmediate - 15.0) / 3.16;
						else if ((age >= 65) && (age < 75))
							storyImmediate_Z_score = (storyImmediate - 14.1) / 3.07;
						else if (age >= 75)
							storyImmediate_Z_score = (storyImmediate - 14.1) / 3.07;
						if (storyImmediate_Z_score != null) {
							addToFact(personId,(long) 2000000209, visitDate, visitFactType, null, storyImmediate_Z_score, null, null, null,  // Story immediate z-score: 2000000209
									UNIT_NA, null, visitOccurrenceId,"",getField("STORY_IMMEDIATE"), null, null, storyImmediate.toString()); 
						}						
					}
					// Story delayed Z score
					if ((storyDelayed != null) && (age != null)) {
						if ((age >= 25) && (age < 35))
							storyDelayed_Z_score = (storyDelayed - 14.9) / 2.33;
						else if ((age >= 35) && (age < 45))
							storyDelayed_Z_score = (storyDelayed - 14.9) / 2.51;
						else if ((age >= 45) && (age < 55))
							storyDelayed_Z_score = (storyDelayed - 16.0) / 1.25;
						else if ((age >= 55) && (age < 65))
							storyDelayed_Z_score = (storyDelayed - 14.9) / 2.81;
						else if ((age >= 65) && (age < 75))
							storyDelayed_Z_score = (storyDelayed - 14.0) / 3.13;
						else if (age >= 75)
							storyDelayed_Z_score = (storyDelayed - 14.0) / 3.13;
						if (storyDelayed_Z_score != null) {
							addToFact(personId,(long) 2000000207, visitDate, visitFactType, null, storyDelayed_Z_score, null, null, null,  // Story delayed z-score: 2000000207
									UNIT_NA, null, visitOccurrenceId,"",getField("STORY_DELAYED"), null, null, storyDelayed.toString()); 
						}						
					}
					
					Long valAsConceptId = null;
					Long apoe4CarrierConceptId = null;
					tmpS = getRowValue(row, "APOE_GENOTYPE");  // E2E2, E2E3, E2E4, E3E3, E3E4, E4E4 (for E2/E2, E2/E3, E4/E4, ...)
					if ((tmpS != null) && (!tmpS.equals(""))) {
						switch (tmpS) {
						case "E2E2":
							valAsConceptId = (long) 2000000240;
							apoe4CarrierConceptId = (long) 2000000248; // Non-carrier
							break;
						case "E2E3":
							valAsConceptId = (long) 2000000241;
							apoe4CarrierConceptId = (long) 2000000248; // Non-carrier
							break;
						case "E2E4":
							valAsConceptId = (long) 2000000242;
							apoe4CarrierConceptId = (long) 2000000247; // Heterozygote
							break;
						case "E3E3":
							valAsConceptId = (long) 2000000243;
							apoe4CarrierConceptId = (long) 2000000248; // Non-carrier
							break;
						case "E3E4":
							valAsConceptId = (long) 2000000244;
							apoe4CarrierConceptId = (long) 2000000247; // Heterozygote
							break;
						case "E4E4":
							valAsConceptId = (long) 2000000245;
							apoe4CarrierConceptId = (long) 2000000246; // Homozygote
							break;
						}
						addToFact(personId,(long) 2000000013, visitDate, visitFactType, null, null, null, null, valAsConceptId,  // APOE genotype: 2000000013
								UNIT_SEE_TABLE, null, visitOccurrenceId,"",getField("APOE_GENOTYPE"), null, null, tmpS); 
						addToFact(personId,(long) 2000000014, visitDate, visitFactType, null, null, null, null, apoe4CarrierConceptId,  // APOE4 carrier: 2000000014
								UNIT_NA, null, visitOccurrenceId,"",getField("APOE_GENOTYPE"), null, null, tmpS); 
					}

					tmpS = getRowValue(row, "DIAGNOSIS");   // 0= geen diagnose, 1=SCI, 2=MCI, 3=dement
					switch (site) {
					case MAASTRICHT:
						switch (tmpS) {
						case "1":
							diagnosisCode = "sci";
							break;
						case "2":
							diagnosisCode = "mci";
							break;
						case "3":
							if (getLongRowValue(row, "AD_FLAG") == 1)
								diagnosisCode = "ad";
							else
								diagnosisCode = null;
							break;
						default:
							diagnosisCode = null;
						}
						break;
					case ANTWERP:
						diagnosisCode = null;
						break;
					}
					if ((diagnosisCode != null) && (!diagnosisCode.equals(""))) {
						conceptId = diagnosisCodeToConcept.get(diagnosisCode);
						addToFact(personId,(long) 2000000063, visitDate, visitFactType, null, null, null, null, conceptId,  // Cognitive disorder diagnosis: 2000000063
								UNIT_NA, null, visitOccurrenceId,"",getField("DIAGNOSIS"), null, null, diagnosisCode); 
					}
					// ========== Assignments - Priority tests ==========
					// Priority attention
					String paRef = null;
					String paZRef = null;
					Long paConceptId = null;
					Long paZConceptId = null;
					Double paScore = null;
					Double paZScore = null;
					Long paFactId = null;
					Long paZFactId = null;
					if (tmtA != null) {
						paRef = "TMTA";
						paScore = tmtA;
						paConceptId = (long) 2000000210; // TMTA
						if (tmtA_Z_score != null) {
							paZRef = "TMTA_Z_SCORE";
							paZScore = tmtA_Z_score;
							paZConceptId = (long) 2000000211; // TMTA Z Score
						}
					} else if (stroop1 != null) {
						paRef = "STROOP1";
						paScore = stroop1;
						paConceptId = (long) 2000000276; // Stroop Part 1
						if (stroop1_Z_score != null) {
							paZRef = "STROOP1_Z_SCORE";
							paZScore = stroop1_Z_score;
							paZConceptId = (long) 2000000277; // Stroop Part 1 - Z Score
						}
					} else if (stroop2 != null) {
						paRef = "STROOP2";
						paScore = stroop2;
						paConceptId = (long) 2000000278; // Stroop Part 2
						if (stroop2_Z_score != null) {
							paZRef = "STROOP2_Z_SCORE";
							paZScore = stroop2_Z_score;
							paZConceptId = (long) 2000000279; // Stroop Part 2 - Z Score
						}
					} // else SDST (Symbol Digit Substitution Test)
					if (paConceptId != null) {
						paFactId = addToFactGetId(personId,(long) 2000000180, visitDate, visitFactType, null, paScore, null, null, null,  // Priority attention: 2000000180
								UNIT_SCORE, null, visitOccurrenceId,"",getField(paRef), null, null, getRowValue(row, paRef)); 
						addToFactAttribute(paFactId,null,null,null,paConceptId,null,(long) 2000000282,null,paRef,null,null,null);
						if (paZConceptId != null) {
							paZFactId = addToFactGetId(personId,(long) 2000000181, visitDate, visitFactType, null, paZScore, null, null, null,  // Priority attention Z-score: 2000000181
									UNIT_SCORE, null, visitOccurrenceId,"",getField(paRef), null, null, getRowValue(row, paRef)); 
							addToFactAttribute(paZFactId,null,null,null,paZConceptId,null,(long) 2000000282,null,paZRef,null,null,null);							
						}
					}

					// Priority executive
					String peRef = null;
					String peZRef = null;
					Long peConceptId = null;
					Long peZConceptId = null;
					Double peScore = null;
					Double peZScore = null;
					Long peFactId = null;
					Long peZFactId = null;
					if (tmtB != null) {
						peRef = "TMTB";
						peScore = tmtB;
						peConceptId = (long) 2000000212; // TMTB
						if (tmtB_Z_score != null) {
							peZRef = "TMTB_Z_SCORE";
							peZScore = tmtB_Z_score;
							peZConceptId = (long) 2000000213; // TMTB Z Score
						}
					} else if (stroop3 != null) {
						peRef = "STROOP3";
						peScore = stroop3;
						peConceptId = (long) 2000000280; // Stroop Part 3
						if (stroop3_Z_score != null) {
							peZRef = "STROOP3_Z_SCORE";
							peZScore = stroop3_Z_score;
							peZConceptId = (long) 2000000281; // Stroop Part 3 - Z Score
						}
					}
					if (peConceptId != null) {
						peFactId = addToFactGetId(personId,(long) 2000000182, visitDate, visitFactType, null, peScore, null, null, null,  // Priority executive: 2000000182
								UNIT_SCORE, null, visitOccurrenceId,"",getField(peRef), null, null, getRowValue(row, peRef)); 
						addToFactAttribute(peFactId,null,null,null,peConceptId,null,(long) 2000000283,null,peRef,null,null,null);
						if (peZConceptId != null) {
							peZFactId = addToFactGetId(personId,(long) 2000000183, visitDate, visitFactType, null, peZScore, null, null, null,  // Priority executive Z-score: 2000000183
									UNIT_SCORE, null, visitOccurrenceId,"",getField(peRef), null, null, getRowValue(row, peRef)); 
							addToFactAttribute(peZFactId,null,null,null,peZConceptId,null,(long) 2000000283,null,peZRef,null,null,null);
						}
					}

					// Priority memory - immediate
					String pmI_Ref = null;
					String pmI_ZRef = null;
					Long pmI_ConceptId = null;
					Long pmI_ZConceptId = null;
					Double pmI_Score = null;
					Double pmI_ZScore = null;
					Long pmI_FactId = null;
					Long pmI_ZFactId = null;
					if (avltImmediate != null) {
						pmI_Ref = "AVLT_IMMEDIATE";
						pmI_Score = avltImmediate;
						pmI_ConceptId = (long) 2000000017; // AVLT immediate
						if (avltImmediate_Z_score != null) {
							pmI_ZRef = "AVLT_IMMEDIATE_Z_SCORE";
							pmI_ZScore = avltImmediate_Z_score;
							pmI_ZConceptId = (long) 2000000018; //	AVLT immediate z-score
						}
					} else if (storyImmediate != null) {
						pmI_Ref = "STORY_IMMEDIATE";
						pmI_Score = storyImmediate;
						pmI_ConceptId = (long) 2000000208; // Story immediate
						if (storyImmediate_Z_score != null) {
							pmI_ZRef = "STORY_IMMEDIATE_Z_SCORE";
							pmI_ZScore = storyImmediate_Z_score;
							pmI_ZConceptId = (long) 2000000209;	// Story immediate z-score
						}
					} else if (vatScore != null) {
						pmI_Ref = "VAT_TOT";
						pmI_Score = vatScore;
						pmI_ConceptId = vatConceptId; // 2000000215: VAT-12, or 2000000216: VAT-24
					} else if (mmseTotal != null) {
						pmI_Ref = "MMSE_TOTAL";
						pmI_Score = mmseTotal;
						pmI_ConceptId = (long) 2000000166; // Mini mental state exam total score
					}
					if (pmI_ConceptId != null) {
						pmI_FactId = addToFactGetId(personId,(long) 2000000188, visitDate, visitFactType, null, pmI_Score, null, null, null,  // Priority memory immediate: 2000000188
								UNIT_SCORE, null, visitOccurrenceId,"",getField(pmI_Ref), null, null, getRowValue(row, pmI_Ref)); 
						addToFactAttribute(pmI_FactId,null,null,null,pmI_ConceptId,null,(long) 2000000285,null,pmI_Ref,null,null,null);
						if (pmI_ZConceptId != null) {
							pmI_ZFactId = addToFactGetId(personId,(long) 2000000189, visitDate, visitFactType, null, pmI_ZScore, null, null, null,  // Priority memory immediate Z score: 2000000189
									UNIT_SCORE, null, visitOccurrenceId,"",getField(pmI_Ref), null, null, getRowValue(row, pmI_Ref)); 
							addToFactAttribute(pmI_ZFactId,null,null,null,pmI_ZConceptId,null,(long) 2000000285,null,pmI_ZRef,null,null,null);
						}
					}

					// Priority memory - delayed
					String pmD_Ref = null;
					String pmD_ZRef = null;
					Long pmD_ConceptId = null;
					Long pmD_ZConceptId = null;
					Double pmD_Score = null;
					Double pmD_ZScore = null;
					Long pmD_FactId = null;
					Long pmD_ZFactId = null;
					if (avltDelayed != null) {
						pmD_Ref = "AVLT_DELAYED";
						pmD_Score = avltDelayed;
						pmD_ConceptId = (long) 2000000015; // AVLT delayed
						if (avltDelayed_Z_score != null) {
							pmD_ZRef = "AVLT_DELAYED_Z_SCORE";
							pmD_ZScore = avltDelayed_Z_score;
							pmD_ZConceptId = (long) 2000000016; //	AVLT delayed z-score
						}
					} else if (storyDelayed != null) {
						pmD_Ref = "STORY_DELAYED";
						pmD_Score = storyDelayed;
						pmD_ConceptId = (long) 2000000206; // Story delayed
						if (storyDelayed_Z_score != null) {
							pmD_ZRef = "STORY_DELAYED_Z_SCORE";
							pmD_ZScore = storyDelayed_Z_score;
							pmD_ZConceptId = (long) 2000000207;	// Story delayed z-score
						}
					} else if (vatScore != null) {
						pmI_Ref = "VAT_TOT";
						pmD_Score = vatScore;
						pmD_ConceptId = vatConceptId; // 2000000215: VAT-12, or 2000000216: VAT-24
					} else if (mmseTotal != null) {
						pmI_Ref = "MMSE_TOTAL";
						pmD_Score = mmseTotal;
						pmD_ConceptId = (long) 2000000166; // Mini mental state exam total score
					}
					if (pmD_ConceptId != null) {
						pmD_FactId = addToFactGetId(personId,(long) 2000000186, visitDate, visitFactType, null, pmD_Score, null, null, null,  // Priority memory delayed: 2000000186
								UNIT_SCORE, null, visitOccurrenceId,"",getField(pmD_Ref), null, null, getRowValue(row, pmD_Ref)); 
						addToFactAttribute(pmD_FactId,null,null,null,pmD_ConceptId,null,(long) 2000000285,null,pmD_Ref,null,null,null);
						if (pmD_ZConceptId != null) {
							pmD_ZFactId = addToFactGetId(personId,(long) 2000000187, visitDate, visitFactType, null, pmD_ZScore, null, null, null,  // Priority memory delayed Z score: 2000000187
									UNIT_SCORE, null, visitOccurrenceId,"",getField(pmD_Ref), null, null, getRowValue(row, pmD_Ref)); 
							addToFactAttribute(pmD_ZFactId,null,null,null,pmD_ZConceptId,null,(long) 2000000285,null,pmD_ZRef,null,null,null);
						}
					}

					// ========== Measurements ==========
					Double tmpDbl = null;
					tmpS = getRowValue(row, "HEIGHT");
					if ((tmpS != null) && (StringUtilities.isNumber(tmpS))) {
						tmpDbl = Double.valueOf(tmpS);
						addToFact(personId,(long) 2000000388, visitDate, visitFactType, null, tmpDbl, null, null, null,  // Length: 2000000388
								UNIT_CM, null, visitOccurrenceId,"",getField("HEIGHT"), null, null, tmpS); 
					}
					tmpS = getRowValue(row, "WEIGHT");
					if ((tmpS != null) && (StringUtilities.isNumber(tmpS))) {
						tmpDbl = Double.valueOf(tmpS);
						addToFact(personId,(long) 2000000462, visitDate, visitFactType, null, tmpDbl, null, null, null,  // Weight: 2000000462
								UNIT_KG, null, visitOccurrenceId,"",getField("WEIGHT"), null, null, tmpS); 
					}
					tmpS = getRowValue(row, "BP_SYSTOLIC");
					if ((tmpS != null) && (StringUtilities.isNumber(tmpS))) {
						tmpDbl = Double.valueOf(tmpS);
						addToFact(personId,(long) 2000000440, visitDate, visitFactType, null, tmpDbl, null, null, null,  // Systolic Blood Pressure: 2000000440
								UNIT_MMHG, null, visitOccurrenceId,"",getField("BP_SYSTOLIC"), null, null, tmpS); 
					}
					tmpS = getRowValue(row, "BP_DIASTOLIC");
					if ((tmpS != null) && (StringUtilities.isNumber(tmpS))) {
						tmpDbl = Double.valueOf(tmpS);
						addToFact(personId,(long) 2000000364, visitDate, visitFactType, null, tmpDbl, null, null, null,  // Diastolic Blood Pressure: 2000000364
								UNIT_MMHG, null, visitOccurrenceId,"",getField("BP_DIASTOLIC"), null, null, tmpS); 
					}
				}
			}
		}
	}

	//********************  ********************//
	private void addCareSiteRecord() {
		String careSiteName = null;
		switch (site) {
		case MAASTRICHT:
			careSiteId = 1;
			careSiteName = "Maastricht";
			break;
		case ANTWERP:
			careSiteId = 2;
			careSiteName = "Antwerp";
			break;
		}

		Row careSite = new Row();
		careSite.add("care_site_id", careSiteId);
		careSite.add("care_site_name", careSiteName);
		careSite.add("place_of_service_concept_id", 9201); // Inpatient visit
		careSite.add("location_id", "");
		careSite.add("care_site_source_value", careSiteName);
		careSite.add("place_of_service_source_value", "");
		tableToRows.put("care_site", careSite);
	}
	
	//********************  ********************//
	private void addToVisitOccurrence(Row row, String visit, String visitDate, Long visitCareSiteId, String sourceVisitRef) {
		Row visitOccurrence = new Row();
		visitOccurrence.add("visit_occurrence_id", visitOccurrenceId);
		visitOccurrence.add("person_id", personId);
		visitOccurrence.add("visit_concept_id", 9201); // Inpatient visit
		visitOccurrence.add("visit_start_date", visitDate != null ? visitDate : "");
		visitOccurrence.add("visit_start_time", "");
		visitOccurrence.add("visit_end_date", visitDate != null ? visitDate : "");
		visitOccurrence.add("visit_end_time", "");
		visitOccurrence.add("visit_type_concept_id", 44818518); // EHR system
		visitOccurrence.add("provider_id", "");
		visitOccurrence.add("care_site_id", (visitCareSiteId != null ? visitCareSiteId.toString() : ""));
		visitOccurrence.add("visit_source_value", visit);
		visitOccurrence.add("visit_source_concept_id", "");
		tableToRows.put("visit_occurrence", visitOccurrence);
		personVisits.add(personId, visitOccurrenceId, visit, visitDate);
	}

	//********************  ********************//
	private void addToObservationPeriod(long personId, String startDate, String endDate) {
		Row observationPeriod = new Row();
		observationPeriod.add("observation_period_id", observationPeriodId);
		observationPeriod.add("person_id", personId);
		observationPeriod.add("observation_period_start_date", startDate);
		observationPeriod.add("observation_period_end_date", endDate != null ? endDate : "");
		observationPeriod.add("period_type_concept_id", 44814724); // Period covering healthcare encounters
		tableToRows.put("observation_period", observationPeriod);
	}

	//********************  ********************//
	private Long addToPerson(Row row) {
		Long newPersonId = maxPersonId++;
		personRefToId.put(getRowValue(row,"PERSON_ID"), newPersonId);
		String tmpG = getRowValue(row,"GENDER").toLowerCase();
		Long genderConceptId = null;
		if ((tmpG != null) && (tmpG.length() > 0))
			switch (tmpG) {
			case "m":
				genderConceptId = GENDER_MALE;
				break;
			case "v":
				genderConceptId = GENDER_FEMALE;
				break;
			default:
				genderConceptId = GENDER_UNKNOWN;
				break;
			}
		Integer age = getPersonAge(row);
		
		Row person = new Row();
		person.add("person_id", newPersonId);
		person.add("gender_concept_id", genderConceptId);

		person.add("year_of_birth", getRowValue(row,"BIRTH_DATE_YEAR"));
		person.add("month_of_birth", getRowValue(row,"BIRTH_DATE_MONTH"));
		person.add("day_of_birth", getRowValue(row,"BIRTH_DATE_DAY"));
		person.add("time_of_birth", "");

		person.add("race_concept_id", 8552); // Unknown race
		person.add("ethnicity_concept_id", "0");
		person.add("location_id", "");
		person.add("provider_id", "");
		person.add("care_site_id", careSiteId);

		person.add("person_source_value", getRowValue(row,"PERSON_ID"));
		person.add("gender_source_value", getRowValue(row,"GENDER"));
		person.add("gender_source_concept_id", "");
		person.add("race_source_value", "");
		person.add("race_source_concept_id", "");
		person.add("ethnicity_source_value", "");
		person.add("ethnicity_source_concept_id", "");

		if (genderConceptId != null)
			personIdToGenderConceptId.put(newPersonId, genderConceptId);
		if (age != null)
			personIdToAge.put(newPersonId, age);

		tableToRows.put("person", person);
		return newPersonId;
	}

	//********************  ********************//
	private Integer getPersonAge(Row row) {
		Integer age = null;

		String tmpS = getRowValue(row, "AGE");
		if ((tmpS != null) && (StringUtilities.isNumber(tmpS))) {
			age = Integer.valueOf(tmpS);
		}
		else {  // calculate age as of today
			Long birthYear = getLongRowValue(row,"BIRTH_DATE_YEAR");
			Long birthMonth = getLongRowValue(row,"BIRTH_DATE_MONTH");
			Long birthDay = getLongRowValue(row,"BIRTH_DATE_DAY");

			if ((birthYear != null) && (birthMonth != null) && (birthDay != null)) {
				DateFormat dateFormat= new SimpleDateFormat("yyyy-MM-dd");
				try {
					Date birthDate = dateFormat.parse(String.format("%04d", birthYear) + "-" + String.format("%02d", birthMonth) + "-" + String.format("%02d", birthDay));
					age = getAge(birthDate);
				} catch (ParseException e) {
					e.printStackTrace();
				}	
			}
		}
		return age;
	}

	//********************  ********************//
//	private void addToMeasurement(
//			long mPersonId,
//			long measurementConceptId,
//			String measurementDate,
//			String measurementTime,
//			long measurementTypeConceptId,
//			Long operatorConceptId,
//			Double valueAsNumber,
//			Long valueAsConceptId,
//			Long unitConceptId,
//			Double rangeLow,
//			Double rangeHigh,
//			Long mProviderId,
//			Long visitOccurrenceId,
//			String measurementSourceValue,
//			Long measurementSourceConceptId,
//			String unitSourceValue,
//			String valueSourceValue) {
//		Row measurement = new Row();
//		measurement.add("measurement_id", ++measurementId);
//		measurement.add("person_id", mPersonId);
//		measurement.add("measurement_concept_id", measurementConceptId);
//		measurement.add("measurement_date", (measurementDate != null ? measurementDate : ""));
//		measurement.add("measurement_time", (measurementTime != null ? measurementTime : ""));
//		measurement.add("measurement_type_concept_id", measurementTypeConceptId);
//		measurement.add("operator_concept_id", (operatorConceptId != null ? operatorConceptId.toString() : ""));
//		measurement.add("value_as_number", (valueAsNumber != null ? valueAsNumber.toString() : ""));
//		measurement.add("value_as_concept_id", (valueAsConceptId != null ? valueAsConceptId.toString() : ""));
//		measurement.add("unit_concept_id", (unitConceptId != null ? unitConceptId.toString() : ""));
//		measurement.add("range_low", (rangeLow != null ? rangeLow.toString() : ""));
//		measurement.add("range_high", (rangeHigh != null ? rangeHigh.toString() : ""));
//		measurement.add("provider_id", (mProviderId != null ? mProviderId.toString() : ""));
//		measurement.add("visit_occurrence_id", (visitOccurrenceId != null ? visitOccurrenceId.toString() : ""));
//		measurement.add("measurement_source_value", measurementSourceValue != null ? measurementSourceValue : "");
//		measurement.add("measurement_source_concept_id", (measurementSourceConceptId != null ? measurementSourceConceptId.toString() : ""));
//		measurement.add("unit_source_value", unitSourceValue != null ? unitSourceValue : "");
//		measurement.add("value_source_value", valueSourceValue != null ? valueSourceValue : "");
//		tableToRows.put("measurement", measurement);
//	}

	//********************  ********************//
//	private void addToMeasurement(
//			long mPersonId,
//			long measurementConceptId,
//			String measurementDate,
//			long measurementTypeConceptId,
//			Double valueAsNumber,
//			Long valueAsConceptId,
//			Long unitConceptId,
//			Long mProviderId,
//			String measurementSourceValue,
//			String unitSourceValue,
//			String valueSourceValue) {
//		//		addToMeasurement(personId, measurementConceptId, measurementDate, measurementTime, measurementTypeConceptId, operatorConceptId, valueAsNumber, valueAsConceptId, unitConceptId, rangeLow, rangeHigh, providerId, visitOccurrenceId, measurementSourceValue, measurementSourceConceptId, unitSourceValue, valueSourceValue);
//		addToMeasurement(mPersonId, measurementConceptId, measurementDate, null, measurementTypeConceptId, null, valueAsNumber, valueAsConceptId, unitConceptId, null, null, mProviderId, visitOccurrenceId, measurementSourceValue, null, unitSourceValue, valueSourceValue);
//	}

	//********************  ********************//
	private void addToFact(
			long fPersonId,
			long factConceptId,
			String factDate,
			long factTypeConceptId,
			Long operatorConceptId,
			Double valueAsNumber,
			String valueAsString,
			String valueAsDate, // yyy-mm-dd
			Long valueAsConceptId,
			Long unitConceptId,
			Long fProviderId,
			Long visitOccurrenceId,
			String comment,
			String factSourceValue,
			Long factSourceConceptId,
			String unitSourceValue,
			String valueSourceValue) {
		Row fact = new Row();
		fact.add("fact_id", ++factId);
		fact.add("person_id", fPersonId);
		fact.add("fact_concept_id", factConceptId);
		fact.add("fact_date", (factDate != null ? factDate : ""));
		fact.add("fact_type_concept_id", factTypeConceptId);
		fact.add("operator_concept_id", (operatorConceptId != null ? operatorConceptId.toString() : ""));
		fact.add("value_as_number", (valueAsNumber != null ? valueAsNumber.toString() : ""));
		fact.add("value_as_string", (valueAsString != null ? valueAsString : ""));
		fact.add("value_as_date", (valueAsDate != null ? valueAsDate : ""));
		fact.add("value_as_concept_id", (valueAsConceptId != null ? valueAsConceptId.toString() : ""));
		fact.add("unit_concept_id", (unitConceptId != null ? unitConceptId.toString() : ""));
		fact.add("provider_id", (fProviderId != null ? fProviderId.toString() : ""));
		fact.add("visit_occurrence_id", (visitOccurrenceId != null ? visitOccurrenceId.toString() : ""));
		fact.add("comment", comment != null ? comment : "");
		fact.add("fact_source_value", factSourceValue != null ? factSourceValue : "");
		fact.add("fact_source_concept_id", (factSourceConceptId != null ? factSourceConceptId.toString() : ""));
		fact.add("unit_source_value", unitSourceValue != null ? unitSourceValue : "");
		fact.add("value_source_value", valueSourceValue != null ? valueSourceValue : "");
		tableToRows.put("sb_fact", fact);
	}

	//********************  ********************//
	private long addToFactGetId(
			long fPersonId,
			long factConceptId,
			String factDate,
			long factTypeConceptId,
			Long operatorConceptId,
			Double valueAsNumber,
			String valueAsString,
			String valueAsDate, // yyy-mm-dd
			Long valueAsConceptId,
			Long unitConceptId,
			Long fProviderId,
			Long visitOccurrenceId,
			String comment,
			String factSourceValue,
			Long factSourceConceptId,
			String unitSourceValue,
			String valueSourceValue) {
		Row fact = new Row();
		fact.add("fact_id", ++factId);
		long returnId = factId;
		fact.add("person_id", fPersonId);
		fact.add("fact_concept_id", factConceptId);
		fact.add("fact_date", (factDate != null ? factDate : ""));
		fact.add("fact_type_concept_id", factTypeConceptId);
		fact.add("operator_concept_id", (operatorConceptId != null ? operatorConceptId.toString() : ""));
		fact.add("value_as_number", (valueAsNumber != null ? valueAsNumber.toString() : ""));
		fact.add("value_as_string", (valueAsString != null ? valueAsString : ""));
		fact.add("value_as_date", (valueAsDate != null ? valueAsDate : ""));
		fact.add("value_as_concept_id", (valueAsConceptId != null ? valueAsConceptId.toString() : ""));
		fact.add("unit_concept_id", (unitConceptId != null ? unitConceptId.toString() : ""));
		fact.add("provider_id", (fProviderId != null ? fProviderId.toString() : ""));
		fact.add("visit_occurrence_id", (visitOccurrenceId != null ? visitOccurrenceId.toString() : ""));
		fact.add("comment", comment != null ? comment : "");
		fact.add("fact_source_value", factSourceValue != null ? factSourceValue : "");
		fact.add("fact_source_concept_id", (factSourceConceptId != null ? factSourceConceptId.toString() : ""));
		fact.add("unit_source_value", unitSourceValue != null ? unitSourceValue : "");
		fact.add("value_source_value", valueSourceValue != null ? valueSourceValue : "");
		tableToRows.put("sb_fact", fact);
		return returnId;
	}

	//********************  ********************//
	private void addToFactAttribute(
			long fFactId,
			Double valueAsNumber,
			String valueAsString,
			String valueAsDate, // yyy-mm-dd
			Long valueAsConceptId,
			Long unitConceptId,
			Long attributeConceptId,
			String comment,
			String factAttributeSourceValue,
			Long factAttributeSourceConceptId,
			String factAttributeUnitSourceValue,
			String factAttributeValueSourceValue) {
		Row factAttribute = new Row();
		factAttribute.add("fact_attribute_id", ++factAttributeId);
		factAttribute.add("fk_fact_id", fFactId);
		factAttribute.add("value_as_number", (valueAsNumber != null ? valueAsNumber.toString() : ""));
		factAttribute.add("value_as_string", (valueAsString != null ? valueAsString : ""));
		factAttribute.add("value_as_date", (valueAsDate != null ? valueAsDate : ""));
		factAttribute.add("value_as_concept_id", (valueAsConceptId != null ? valueAsConceptId.toString() : ""));
		factAttribute.add("unit_concept_id", (unitConceptId != null ? unitConceptId.toString() : ""));
		factAttribute.add("attribute_concept_id", (attributeConceptId != null ? attributeConceptId.toString() : ""));
		factAttribute.add("comment", comment != null ? comment : "");
		factAttribute.add("fact_attribute_source_value", factAttributeSourceValue != null ? factAttributeSourceValue : "");
		factAttribute.add("fact_attribute_source_concept_id", (factAttributeSourceConceptId != null ? factAttributeSourceConceptId.toString() : ""));
		factAttribute.add("fact_attribute_unit_source_value", factAttributeUnitSourceValue != null ? factAttributeUnitSourceValue : "");
		factAttribute.add("fact_attribute_value_source_value", factAttributeValueSourceValue != null ? factAttributeValueSourceValue : "");
		tableToRows.put("sb_fact_attribute", factAttribute);
	}

	//********************  ********************//
	private static boolean isADateString(String dateStr) {
		boolean isDate = false;
		checkLabel:
			if (dateStr.length() > 7) {
				if ((dateStr.length() - dateStr.replace("/", "").length()) == 2) {
					if (StringUtilities.isNumber(dateStr.replace("/", ""))) {
						isDate = true;
						try {
							DateFormat df= new SimpleDateFormat("yyyy/MM/dd");
				            df.setLenient(false);
				            df.parse(dateStr);
						} catch (ParseException e) {
							isDate = false;
						}
						break checkLabel;
					}
				}
				else 
					if ((dateStr.length() - dateStr.replace("-", "").length()) == 2) {
						if (StringUtilities.isNumber(dateStr.replace("-", ""))) {
							isDate = true;
							try {
								DateFormat df= new SimpleDateFormat("yyyy-MM-dd");
					            df.setLenient(false);
					            df.parse(dateStr);
							} catch (ParseException e) {
								isDate = false;
							}
							break checkLabel;
						}
					}
			}
		return isDate;
	}

	
	//********************  ********************//
	public static int getAge(Date dateOfBirth) {

	    Calendar today = Calendar.getInstance();
	    Calendar birthDate = Calendar.getInstance();

	    int age = 0;

	    birthDate.setTime(dateOfBirth);
	    if (birthDate.after(today)) {
	        throw new IllegalArgumentException("Can't be born in the future");
	    }

	    age = today.get(Calendar.YEAR) - birthDate.get(Calendar.YEAR);

	    // If birth date is greater than todays date (after 2 days adjustment of leap year) then decrement age one year   
	    if ( (birthDate.get(Calendar.DAY_OF_YEAR) - today.get(Calendar.DAY_OF_YEAR) > 3) ||
	            (birthDate.get(Calendar.MONTH) > today.get(Calendar.MONTH ))){
	        age--;

	     // If birth date and todays date are of same month and birth day of month is greater than todays day of month then decrement age
	    }else if ((birthDate.get(Calendar.MONTH) == today.get(Calendar.MONTH )) &&
	              (birthDate.get(Calendar.DAY_OF_MONTH) > today.get(Calendar.DAY_OF_MONTH ))){
	        age--;
	    }

	    return age;
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
					if (row.getFieldNames().contains(getField("PERSON_ID")))
						etlReport.reportProblem(table, "Column " + nonAllowedNullField + " is null, could not create row", getRowValue(row, "PERSON_ID"));
					else
						etlReport.reportProblem(table, "Column " + nonAllowedNullField + " is null, could not create row", "");
					iterator.remove();
				}
			}
		}
	}

	//********************  ********************//
	private void createCustomTablesAndConcepts(DbSettings dbSettings) {
		String sqlFile = "sql/CreateFactTablesConcepts - SQL Server.sql";
		if (dbSettings.dbType == DbType.ORACLE) {
			sqlFile = "sql/CreateFactTablesConcepts - Oracle.sql";
		} else if (dbSettings.dbType == DbType.MSSQL) {
			sqlFile = "sql/CreateFactTablesConcepts - SQL Server.sql";
		} else if (dbSettings.dbType == DbType.POSTGRESQL) {
			sqlFile = "sql/CreateFactTablesConcepts - PostgreSQL.sql";
		}
		connection.executeResource(sqlFile);
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
	private String getField(String key) {
		String fieldName;
		try {
			fieldName = keyToFieldName.get(key.toLowerCase()).toUpperCase();
		} catch (Exception e) {
			fieldName = null;
		}
		return fieldName;
	}

	//********************  ********************//
	private String getRowValue(Row row, String key) {
		String field = getField(key);
		if ((field != null) && (field.length() > 0))
			return row.get(field);
		else
			return null;
	}
	//********************  ********************//
	private Long getLongRowValue(Row row, String key) {
		String field = getField(key);
		if ((field != null) && (field.length() > 0))
			return row.getLong(field);
		else
			return null;
	}
	//********************  ********************//
	public String getFolder() {
		return folder;
	}

	//********************  ********************//
	public void setFolder(String folder) {
		this.folder = folder;
	}

}
