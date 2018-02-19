SELECT 
	REPLACE(sourcev.concept_code, '.', '') AS source_code,
	sourcev.concept_id AS source_concept_id,
	sourcev.concept_name AS source_name,
	sourcev.vocabulary_id AS source_vocabulary_id,
	sourcev.domain_id AS source_domain_id,
	targetv.concept_code AS target_code,
	targetv.concept_name AS target_name,
	targetv.concept_id AS target_concept_id,
	targetv.vocabulary_id AS target_vocabulary_id,
	targetv.domain_id AS target_domain_id,
	targetv.standard_concept AS target_standard,
	targetv.valid_start_date AS target_valid_start_date,
	targetv.valid_end_date AS target_valid_end_date,
	targetv.invalid_reason AS target_invalid_reason
FROM concept sourcev
INNER JOIN concept_relationship
	ON sourcev.concept_id = concept_id_1
INNER JOIN concept targetv
	ON targetv.concept_id = concept_id_2
WHERE sourcev.vocabulary_id = '__SOURCEVOCAB__'
	AND replace(sourcev.concept_code,'.','') = '__SOURCECODE__'
	AND targetv.standard_concept = 'S'
	AND relationship_id = 'Maps to'
	AND (concept_relationship.invalid_reason IS NULL OR concept_relationship.invalid_reason = '')
