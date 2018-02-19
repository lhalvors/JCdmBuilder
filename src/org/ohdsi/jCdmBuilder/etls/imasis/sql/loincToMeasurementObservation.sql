SELECT 
	REPLACE(loinc.concept_code, '.', '') AS source_code,
	loinc.concept_name AS source_name,
	loinc.concept_id AS source_concept_id,
	standard.concept_code AS target_code,
	standard.concept_name AS target_name,
	standard.concept_id AS target_concept_id,
	standard.domain_id
FROM concept loinc
INNER JOIN concept_relationship
	ON loinc.concept_id = concept_id_1
INNER JOIN concept standard
	ON standard.concept_id = concept_id_2
WHERE loinc.vocabulary_id = 'LOINC'
	AND standard.standard_concept = 'S'
	AND relationship_id = 'Maps to'
	AND (concept_relationship.invalid_reason IS NULL OR concept_relationship.invalid_reason = '')
