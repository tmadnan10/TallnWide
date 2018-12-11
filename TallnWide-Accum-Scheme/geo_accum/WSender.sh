ID=$1
shift
for i in "$@"
do 
    ./sendInitW.sh "$ID" "$i" &
done
echo done
