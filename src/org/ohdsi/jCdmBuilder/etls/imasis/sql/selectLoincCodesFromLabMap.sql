select concat(cast(lrn.lab_result_name_id as char),'_',cast(ltn.lab_test_name_id as char)) as res_test_name, lm.loinc_id
from lab_map lm, lab_result_name lrn, lab_test_name ltn
where lm.lab_result_name = lrn.lab_result_name
and lm.lab_test_number = ltn.lab_test_number
order by lm.lab_result_name, lm.lab_test_number;
