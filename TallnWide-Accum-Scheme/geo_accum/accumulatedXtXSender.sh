for i in "$@"
do
        ./sendAccumulatedXtX.sh "$i" &
done
echo done
