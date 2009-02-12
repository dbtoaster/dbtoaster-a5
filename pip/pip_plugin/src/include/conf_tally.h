//////////////////////////////////////////////////////////////////////////
// conf_tally.h
// 
// A conf tally is effectively an array of integers used to tally the 
// number of rows that satisfy a given sample set.  
// 
//////////////////////////////////////////////////////////////////////////

#ifndef PIP_CONF_TALLY_H
#define PIP_CONF_TALLY_H

#define TALLY_GROUP ((pip_conf_tally_group *)tally->data)
#define TALLY_COUNT ((int32 *)(tally->data + sizeof(pip_conf_tally_group) * tally->group_cnt))

pip_conf_tally *pip_conf_tally_create(int32 sample_count, int group_count);
pip_conf_tally *pip_conf_tally_addgroup(pip_conf_tally *tally, int64 ssid, int sample_cnt);
bool pip_conf_tally_up(pip_conf_tally *tally, int64 ssid, int i);
float8 pip_conf_tally_compute_result(pip_conf_tally *tally);

#endif
