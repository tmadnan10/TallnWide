touch doneInit
for i in "$@"
do
    ./sendDoneInit.sh "$i" &
done
