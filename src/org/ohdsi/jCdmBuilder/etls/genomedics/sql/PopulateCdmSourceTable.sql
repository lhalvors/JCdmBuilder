INSERT INTO cdm_source (
	cdm_source_name,
	cdm_source_abbreviation,
	cdm_holder,
	source_description,
	source_documentation_reference,
	cdm_etl_reference,
	source_release_date,
	cdm_release_date,
	cdm_version,
	vocabulary_version
	)
VALUES (
	'IMS Health Longitudinal Patient Database',
	'IMS_HSD_LPD',
	'Genomedics srlu',
	'Primary Care Database See DoA',
	'',
	'https://github.com/OHDSI/JCdmBuilder',
	NULL,
	'@today',
	'5.0',
	'5.0'
	);