idx=$1
shift
rnd=$1
shift
for i in "$@"
    do ./sendAccumulatedW.sh "$idx" "$i" "$rnd" &
done
echo done
