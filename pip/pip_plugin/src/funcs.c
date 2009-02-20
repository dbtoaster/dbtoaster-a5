#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "pip.h"
#include "funcs.h"
PG_FUNCTION_INFO_V1(conf_one);
PG_FUNCTION_INFO_V1(pip_atom_conf_sample_g);
PG_FUNCTION_INFO_V1(pip_atom_sample_set_presence);
PG_FUNCTION_INFO_V1(pip_atom_in);
PG_FUNCTION_INFO_V1(pip_atom_out);
PG_FUNCTION_INFO_V1(pip_atom_create_gt_ee);
PG_FUNCTION_INFO_V1(pip_atom_create_lt_ee);
PG_FUNCTION_INFO_V1(pip_atom_create_gt_ef);
PG_FUNCTION_INFO_V1(pip_atom_create_gt_fe);
PG_FUNCTION_INFO_V1(pip_atom_create_lt_ef);
PG_FUNCTION_INFO_V1(pip_atom_create_lt_fe);
PG_FUNCTION_INFO_V1(pip_conf_tally_in);
PG_FUNCTION_INFO_V1(pip_conf_tally_out);
PG_FUNCTION_INFO_V1(pip_conf_tally_result);
PG_FUNCTION_INFO_V1(pip_eqn_in);
PG_FUNCTION_INFO_V1(pip_eqn_out);
PG_FUNCTION_INFO_V1(pip_expectation);
PG_FUNCTION_INFO_V1(pip_expectation_max_g);
PG_FUNCTION_INFO_V1(pip_expectation_sum_g);
PG_FUNCTION_INFO_V1(pip_expectation_sum_g_one);
PG_FUNCTION_INFO_V1(pip_eqn_sum_ee);
PG_FUNCTION_INFO_V1(pip_eqn_sum_ei);
PG_FUNCTION_INFO_V1(pip_eqn_sum_ie);
PG_FUNCTION_INFO_V1(pip_eqn_sum_ef);
PG_FUNCTION_INFO_V1(pip_eqn_sum_fe);
PG_FUNCTION_INFO_V1(pip_eqn_mul_ee);
PG_FUNCTION_INFO_V1(pip_eqn_mul_ei);
PG_FUNCTION_INFO_V1(pip_eqn_mul_ie);
PG_FUNCTION_INFO_V1(pip_eqn_mul_ef);
PG_FUNCTION_INFO_V1(pip_eqn_mul_fe);
PG_FUNCTION_INFO_V1(pip_eqn_neg);
PG_FUNCTION_INFO_V1(pip_eqn_sub_ee);
PG_FUNCTION_INFO_V1(pip_eqn_sub_ei);
PG_FUNCTION_INFO_V1(pip_eqn_sub_ie);
PG_FUNCTION_INFO_V1(pip_eqn_sub_ef);
PG_FUNCTION_INFO_V1(pip_eqn_sub_fe);
PG_FUNCTION_INFO_V1(pip_eqn_structural_equals);
PG_FUNCTION_INFO_V1(pip_eqn_subscript);
PG_FUNCTION_INFO_V1(pip_exp_in);
PG_FUNCTION_INFO_V1(pip_exp_out);
PG_FUNCTION_INFO_V1(pip_exp_make);
PG_FUNCTION_INFO_V1(pip_exp_fix);
PG_FUNCTION_INFO_V1(pip_exp_expect);
PG_FUNCTION_INFO_V1(pip_presampler_in);
PG_FUNCTION_INFO_V1(pip_presampler_out);
PG_FUNCTION_INFO_V1(pip_presampler_create);
PG_FUNCTION_INFO_V1(pip_presampler_advance);
PG_FUNCTION_INFO_V1(pip_presampler_get);
PG_FUNCTION_INFO_V1(pip_sample_set_in);
PG_FUNCTION_INFO_V1(pip_sample_set_out);
PG_FUNCTION_INFO_V1(pip_sample_set_generate);
PG_FUNCTION_INFO_V1(pip_sample_set_explode);
PG_FUNCTION_INFO_V1(pip_sample_set_expect);
PG_FUNCTION_INFO_V1(pip_value_bundle_in);
PG_FUNCTION_INFO_V1(pip_value_bundle_out);
PG_FUNCTION_INFO_V1(pip_value_bundle_create);
PG_FUNCTION_INFO_V1(pip_value_bundle_cmp_vv);
PG_FUNCTION_INFO_V1(pip_value_bundle_cmp_vi);
PG_FUNCTION_INFO_V1(pip_value_bundle_cmp_iv);
PG_FUNCTION_INFO_V1(pip_value_bundle_cmp_vf);
PG_FUNCTION_INFO_V1(pip_value_bundle_cmp_fv);
PG_FUNCTION_INFO_V1(pip_value_bundle_add_vf);
PG_FUNCTION_INFO_V1(pip_value_bundle_add_vv);
PG_FUNCTION_INFO_V1(pip_value_bundle_mul_vf);
PG_FUNCTION_INFO_V1(pip_value_bundle_mul_vv);
PG_FUNCTION_INFO_V1(pip_value_bundle_expect);
PG_FUNCTION_INFO_V1(pip_value_bundle_max);
PG_FUNCTION_INFO_V1(pip_var_in);
PG_FUNCTION_INFO_V1(pip_var_out);
PG_FUNCTION_INFO_V1(pip_var_create_str);
PG_FUNCTION_INFO_V1(pip_var_create_row);
PG_FUNCTION_INFO_V1(pip_world_presence_in);
PG_FUNCTION_INFO_V1(pip_world_presence_out);
PG_FUNCTION_INFO_V1(pip_world_presence_create);
PG_FUNCTION_INFO_V1(pip_world_presence_count);
PG_FUNCTION_INFO_V1(pip_world_presence_union);
#include "funcs/integration.c"
#include "funcs/pip_atom.c"
#include "funcs/pip_conf_tally.c"
#include "funcs/pip_eqn.c"
#include "funcs/pip_expectation.c"
#include "funcs/pip_presampler.c"
#include "funcs/pip_sample_set.c"
#include "funcs/pip_value_bundle.c"
#include "funcs/pip_var.c"
#include "funcs/pip_world_presence.c"
