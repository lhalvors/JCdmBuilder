CREATE TABLE cost (
     cost_id					INTEGER	    NOT NULL , 
     cost_event_id       INTEGER     NOT NULL ,
     cost_domain_id       VARCHAR(20)    NOT NULL ,
     cost_type_concept_id       INTEGER     NOT NULL ,
     currency_concept_id			INTEGER			NULL ,
     total_charge						FLOAT			NULL , 
     total_cost						FLOAT			NULL , 
     total_paid						FLOAT			NULL , 
     paid_by_payer					FLOAT			NULL , 
     paid_by_patient						FLOAT			NULL , 
     paid_patient_copay						FLOAT			NULL , 
     paid_patient_coinsurance				FLOAT			NULL , 
     paid_patient_deductible			FLOAT			NULL , 
     paid_by_primary						FLOAT			NULL , 
     paid_ingredient_cost				FLOAT			NULL , 
     paid_dispensing_fee					FLOAT			NULL , 
     payer_plan_period_id			INTEGER			NULL ,
     amount_allowed		FLOAT			NULL , 
     revenue_code_concept_id		INTEGER			NULL , 
     revenue_code_source_value    VARCHAR(50)    NULL
    )  ON [PRIMARY]
WITH(DATA_COMPRESSION = PAGE);
