select 
	icd9.concept_code as source_code,
	icd9.concept_name as source_name,
	icd9.concept_id as source_concept_id,
	standard.concept_code as target_code,
	standard.concept_name as target_name,
	standard.concept_id as target_concept_id,
	standard.domain_id
from concept icd9
inner join concept_relationship
	on icd9.concept_id = concept_id_1
inner join concept standard
	on standard.concept_id = concept_id_2
where icd9.vocabulary_id = 'icd9cm'
	and standard.standard_concept = 's'
	and relationship_id = 'maps to'
	and (concept_relationship.invalid_reason is null or concept_relationship.invalid_reason = '')
