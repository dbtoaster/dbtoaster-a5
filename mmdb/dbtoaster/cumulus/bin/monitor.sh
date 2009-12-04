#!/bin/bash
watch "ps auxww | grep ruby | grep -v grep | grep -v sed | sed 's/-I [^ ]* //g; s:ruby [^ ]*/\\([^/ \\-]*\\)-launcher.rb .*:\\1 :' | awk '{print \$11,\$3,\$2,\$5,\$6}' | column -t"
