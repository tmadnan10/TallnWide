while read -r line
do
    for i in $line
    do
        echo "$i"
	rm *XtX"$i"
    done
done < nodes
