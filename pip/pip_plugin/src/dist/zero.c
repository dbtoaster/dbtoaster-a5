#include <stdlib.h>
#include "pip.h"

float8  pip_zero_gen  (pip_var *var, int64 seed);
float8  pip_zero_pdf  (pip_var *var, float8 point);

//the script gen_disttable.sh treats this file specially, as the zero distribution must be at index 0
//consequently, if you modify the following line, be sure to modify the shellscript too
DECLARE_PIP_DISTRIBUTION(zero) = {
  .name = "Zero",
  .size = 0,
  .init= NULL,
  .gen = &pip_zero_gen,
  .pdf = &pip_zero_pdf,
  .cdf = NULL,
  .icdf= NULL,
  .in  = NULL,
  .out = NULL,
  .joint= false
};
  
float8  pip_zero_gen  (pip_var *var, int64 seed)
{
  return 0.0;
}
float8  pip_zero_pdf  (pip_var *var, float8 point)
{
  return ( point == 0.0 ) ? ( 1.0 ) : ( 0.0 );
}
