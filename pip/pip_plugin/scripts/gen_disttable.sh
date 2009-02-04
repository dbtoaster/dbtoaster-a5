#!/bin/bash

echo '#include "pip.h"' > `dirname $0`/../src/disttable.c

# Generate some external references to the definitions
grep -rh DECLARE_PIP_DISTRIBUTION `dirname $0`/../src/dist/*.c |
  sed 's/[^(]*(\([^)]*\)).*/extern pip_functable_entry \1_functable;/' >> `dirname $0`/../src/disttable.c

# Generate the distribution table
# Note that because we treat entry 0 as special, it is imperative
#  that the zero entry be included manually as the first.  We also
#  use this opportunity to normalize the ','s
echo "pip_functable_entry *pip_distributions[] = {" >> `dirname $0`/../src/disttable.c
echo "&zero_functable" >> `dirname $0`/../src/disttable.c
grep -rh DECLARE_PIP_DISTRIBUTION `dirname $0`/../src/dist/*.c | 
  grep -v '(zero)' |
  sed 's/[^(]*(\([^)]*\)).*/,\& \1_functable/' >> `dirname $0`/../src/disttable.c
echo "};" >> `dirname $0`/../src/disttable.c

# And finally compute the number of entries
echo "int pip_distribution_count = " $(grep -rh DECLARE_PIP_DISTRIBUTION `dirname $0`/../src/dist/*.c | wc -l) ";" >> `dirname $0`/../src/disttable.c

echo >> `dirname $0`/../src/disttable.c