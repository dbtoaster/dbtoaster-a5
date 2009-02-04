#include "pip.h"
extern pip_functable_entry normal_functable;
extern pip_functable_entry poisson_functable;
extern pip_functable_entry zero_functable;
pip_functable_entry *pip_distributions[] = {
&zero_functable
,& normal_functable
,& poisson_functable
};
int pip_distribution_count =  3 ;

