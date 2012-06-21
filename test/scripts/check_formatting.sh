#!/bin/bash

get_max_column() {
    retval=`awk 'BEGIN{max=0; ln=0} \
                 { if (length > max) {max=length; ln=NR} } \
                 END{print max,ln}' $1`
    width=`echo "$retval" | awk '{print $1}'`
    line=`echo "$retval" | awk '{print $2}'`
}

not_within_column_limit() {
    get_max_column $1
    if [ $width -gt 80 ]
        then printf 'Line: %-5s  Width: %-4s in %s\n' $line $width $1
    fi
}

export -f get_max_column
export -f not_within_column_limit

echo "********** TAB CHECK **********"
find src test lib -type f \( -name '*.ml' -o -name '*.mll' -o -name '*.mly' -o \
                             -name '*.cpp' -o -name '*.hpp' -o -name '*.scala' \) \
     -exec grep -l $'\t' {} ";"

echo 
echo "******** COLUMN CHECK *********"
find src test lib -type f \( -name '*.ml' -o -name '*.mll' -o -name '*.mly' -o \
                             -name '*.cpp' -o -name '*.hpp' -o -name '*.scala' \) \
     -exec bash -c 'not_within_column_limit {}' ";"

