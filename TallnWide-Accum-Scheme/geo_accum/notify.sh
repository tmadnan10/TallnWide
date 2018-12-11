ssh $1 << EOF 
#cd accum/
./notifyParent.sh geo_accum $2
EOF
exit
