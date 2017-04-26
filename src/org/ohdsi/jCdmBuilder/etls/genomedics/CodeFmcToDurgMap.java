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

import java.util.HashMap;
import java.util.Map;

import org.ohdsi.utilities.collections.CountingSet;

public class CodeFmcToDurgMap {

	private String					name;
	private Map<String, CodeData>	codeToData	= new HashMap<String, CodeData>();
	private CountingSet<String>		codeCounts	= new CountingSet<String>();

	public CodeFmcToDurgMap(String name) {
		this.name = name;
	}

	public void add(String code, String description, String atc, String via, Double quant_pa, Double unita_posologica, String unita_mis_ddd, Double giorni) {
		CodeData data = codeToData.get(code);
		if (data == null) {
			data = new CodeData();
			data.description = description;
			data.atc = atc;
			data.via = via;
			data.quant_pa = quant_pa;
			data.unita_posologica = unita_posologica;
			data.unita_mis_ddd = unita_mis_ddd;
			data.giorni = giorni;
			codeToData.put(code, data);
		} else {
			// error
		}
	}

	public String getName() {
		return name;
	}

	public String getDescription(String code) {
		codeCounts.add(code);
		CodeData data = codeToData.get(code);
		if (data == null) {
			return "";
		} else
			return data.description;
	}

	public String getATC(String code) {
		codeCounts.add(code);
		CodeData data = codeToData.get(code);
		if (data == null) {
			return "";
		} else
			return data.atc;
	}
	
	public String getVia(String code) {
		codeCounts.add(code);
		CodeData data = codeToData.get(code);
		if (data == null) {
			return "";
		} else
			return data.via;
	}


	public Double getQuantPa(String code) {
		codeCounts.add(code);
		CodeData data = codeToData.get(code);
		if (data == null) {
			return null;
		} else
			return data.quant_pa;
	}

	public Double getUnitaPosologica(String code) {
		codeCounts.add(code);
		CodeData data = codeToData.get(code);
		if (data == null)
			return null;
		else
			return data.unita_posologica;
	}

	public String getUnitaMisDDD(String code) {
		codeCounts.add(code);
		CodeData data = codeToData.get(code);
		if (data == null) {
			return "";
		} else
			return data.unita_mis_ddd;
	}

	public Double getGiorni(String code) {
		codeCounts.add(code);
		CodeData data = codeToData.get(code);
		if (data == null) {
			return null;
		} else
			return data.giorni;
	}

	public CountingSet<String> getCodeCounts() {
		return codeCounts;
	}

	public CodeData getCodeData(String code) {
		return codeToData.get(code);
	}

	public class CodeData {
		public String	description;
		public String	atc;
		public String	via;
		public Double	quant_pa;
		public Double	unita_posologica;
		public String	unita_mis_ddd;
		public Double	giorni;
	}
}
