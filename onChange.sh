#!/bin/sh  
f=$1  
shift  
tmpf="`mktemp /tmp/onchange.XXXXX`"  
cp "$f" "$tmpf"  
trap "rm $tmpf; exit 1" 2  
while : ; do  
    if [ "$f" -nt "$tmpf" ]; then  
        touch -r "$f" "$tmpf"  
        "$@" &
    fi  
    sleep 2  
done 

