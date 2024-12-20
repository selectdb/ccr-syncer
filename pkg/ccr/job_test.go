package ccr_test

import (
	"testing"

	"github.com/selectdb/ccr_syncer/pkg/ccr"
)

func TestIsSessionVariableRequired(t *testing.T) {
	tests := []string{
		"If you want to specify column names, please `set enable_nereids_planner=true`",
		"set enable_variant_access_in_original_planner = true in session variable",
		"Please enable the session variable 'enable_projection' through `set enable_projection = true",
		"agg state not enable, need set enable_agg_state=true",
		"which is greater than 38 is disabled by default. set enable_decimal256 = true to enable it",
		"if we have a column with decimalv3 type and set enable_decimal_conversion = false",
		"Incorrect column name '名称'. Column regex is '^[_a-zA-Z@0-9\\s/][.a-zA-Z0-9_+-/?@#$%^&*\"\\s,:]{0,255}$'",
	}
	for i, test := range tests {
		if !ccr.IsSessionVariableRequired(test) {
			t.Errorf("test %d failed, input: %s", i, test)
		}
	}
}
