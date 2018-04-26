select
	loinc.concept_code as source_code,
	loinc.concept_name as source_name,
	loinc.concept_id as source_concept_id,
	standard.concept_code as target_code,
	standard.concept_name as target_name,
	standard.concept_id as target_concept_id,
	standard.domain_id
from concept loinc
inner join concept_relationship
	on loinc.concept_id = concept_id_1
inner join concept standard
	on standard.concept_id = concept_id_2
where loinc.vocabulary_id = 'LOINC'
	and standard.standard_concept = 'S'
	and relationship_id = 'Maps to'
	and (concept_relationship.invalid_reason is null or concept_relationship.invalid_reason = '')
	