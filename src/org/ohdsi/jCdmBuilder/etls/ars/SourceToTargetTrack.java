package org.ohdsi.jCdmBuilder.etls.ars;

import java.io.File;
import java.util.ArrayList;

import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.files.WriteTextFile;

public class SourceToTargetTrack {

	private String			folder;
	private String			trackFile;
	private WriteTextFile 	trackOut = null;
	private ArrayList<SourceToTargetItem>	items = new ArrayList<SourceToTargetItem>();
	
	//********************  ********************//

	public SourceToTargetTrack(String folder) {
		this.folder = folder;
		setUpTrackFile();
	}
	
	//********************  ********************//
	
	private void setUpTrackFile() {
		trackFile = folder + "/SourceToTargetLog.tsv";
		int i = 1;
		while (new File(trackFile).exists())
			trackFile = folder + "/SourceToTargetLog" + (i++) + ".tsv";
		trackOut = new WriteTextFile(trackFile);
		trackOut.writeln("Source"+"\t"+"Source_table"+"\t"+"Source_code"+"\t"+"Source_name"+"\t"+"Source_vocabulary"+"\t"+"Source_concept_ID"+"\t"+"Target_table"+"\t"+"Target_code"+"\t"+"Target_name"+"\t"+"Target_domain"+"\t"+"Target_vocabulary"+"\t"+"Target_concept_ID");
	}

	//********************  ********************//
	
	public void add(String srcInstance, 
					String srcTable, 
					String srcCode, 
					String srcName, 
					String srcVocabulary, 
					Long 	srcConceptId, 
					String trgTable, 
					String trgCode, 
					String trgName, 
					String trgDomain, 
					String trgVocabulary, 
					Long 	trgConceptId) {
		SourceToTargetItem newItem = new SourceToTargetItem();
		newItem.sourceInstance = srcInstance;
		newItem.sourceTable = srcTable;
		newItem.sourceCode = srcCode;
		newItem.sourceName = srcName;
		newItem.sourceVocabulary = srcVocabulary;
		newItem.sourceConceptId = srcConceptId;
		newItem.targetTable = trgTable;
		newItem.targetCode = trgCode;
		newItem.targetName = trgName;
		newItem.targetDomain = trgDomain;
		newItem.targetVocabulary = trgVocabulary;
		newItem.targetConceptId = trgConceptId;
		items.add(newItem);
		}
	
	//********************  ********************//
	
	public void writeBatch() {
		if (items.size() > 0) {
			for (SourceToTargetItem item : items) {
				String itemLine = item.sourceInstance + "\t";
				itemLine += item.sourceTable + "\t";
				itemLine += item.sourceCode + "\t";
				itemLine += item.sourceName + "\t";
				itemLine += item.sourceVocabulary + "\t";
				itemLine += item.sourceConceptId + "\t";
				itemLine += item.targetTable + "\t";
				itemLine += item.targetCode + "\t";
				itemLine += item.targetName + "\t";
				itemLine += item.targetDomain + "\t";
				itemLine += item.targetVocabulary + "\t";
				itemLine += item.targetConceptId;
				trackOut.writeln(itemLine);
			}
			items.clear();
		}
	}
	
	//********************  ********************//

	public void finalize() {
		writeBatch();
		trackOut.close();
	}
	
	//********************  ********************//

	public class SourceToTargetItem {
		public String	sourceInstance;
		public String	sourceTable;
		public String	sourceCode;
		public String	sourceName;
		public String	sourceVocabulary;
		public Long		sourceConceptId;
		public String	targetTable;
		public String	targetCode;
		public String	targetName;
		public String	targetDomain;
		public String	targetVocabulary;
		public Long		targetConceptId;
	}
}
