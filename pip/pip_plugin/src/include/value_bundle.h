//////////////////////////////////////////////////////////////////////////
// value_bundle.h
// 
// A value bundle is an array of pregenerated values for a given variable.
// These are used exclusively for emulating Sample-First.
// 
//////////////////////////////////////////////////////////////////////////

#ifndef PIP_VALUE_BUNDLE_H
#define PIP_VALUE_BUNDLE_H

#define PIP_VB_VAR(valB) ((pip_var *)valB->data)
#define PIP_VB_VAL(valB) ((float8 *)(valB->data + VARSIZE((pip_var *)valB->data)))

pip_value_bundle *pip_value_bundle_alloc(pip_var *var, int worldcount, int64 seed);


#endif
