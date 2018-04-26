select rs.national_code_cima_id, rs.rxnorm_code, rs.rxnorm_str
from (
	SELECT
	national_code_cima_id,
	rxnorm_code,
	rxnorm_str,
	ttype_rxnorm,
	IF(national_code_cima_id = @last, @curRank := NULL, @curRank := @_sequence) AS rank,
	@_sequence := @_sequence + 1,
	@last := national_code_cima_id
	FROM natcode_to_rxnorm, (SELECT @curRank := 1, @_sequence := 1, @last := 0) r
	ORDER BY national_code_cima_id, ttype_rxnorm) as rs
WHERE rs.rank is not null;