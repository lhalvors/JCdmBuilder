package org.ohdsi.jCdmBuilder.etls.switchbox;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.ohdsi.utilities.collections.CountingSet;

public class PersonVisitMap {

	private String							name;
	private Map<String, PersonDomainData>	personToData	= new HashMap<String, PersonDomainData>();
	private CountingSet<String>				personCounts	= new CountingSet<String>();
	private PersonDomainData				unmappedData;

	public PersonVisitMap(String name) {
		this.name = name;
		unmappedData = new PersonDomainData();
		unmappedData.personId = 0;
		TargetVisit targetConcept = new TargetVisit();
		targetConcept.visitLabel = "";
		targetConcept.visitOccurrenceId = 0;
		targetConcept.visitDate = "";
		unmappedData.targetVisits.add(targetConcept);
	}

	public void add(long personId, long visitOccurrenceId, String visitLabel, String visitDate) {
		PersonDomainData data = personToData.get(Long.toString(personId));
		if (data == null) {
			data = new PersonDomainData();
			data.personId = personId;
			personToData.put(Long.toString(personId), data);
		}
		TargetVisit targetConcept = new TargetVisit();
		targetConcept.visitLabel = visitLabel;
		targetConcept.visitOccurrenceId = visitOccurrenceId;
		targetConcept.visitDate = visitDate;
		data.targetVisits.add(targetConcept);

	}

	public String getName() {
		return name;
	}

	public CountingSet<String> getPersonCounts() {
		return personCounts;
	}

	public PersonDomainData getPersonData(Long personId) {
		personCounts.add(Long.toString(personId));
		PersonDomainData data = personToData.get(Long.toString(personId));
		if (data == null) {
			return unmappedData;
		} else
			return data;
	}

	public class PersonDomainData {
		public long					personId;
		public List<TargetVisit>	targetVisits	= new ArrayList<PersonVisitMap.TargetVisit>();
	}

	public class TargetVisit {
		public long		visitOccurrenceId;
		public String	visitLabel;
		public String	visitDate;
	}

	public boolean hasMapping(String person) {
		return personToData.containsKey(person);
	}
}
