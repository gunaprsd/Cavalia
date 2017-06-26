FOLDER="/home/guna/results/experiments-6-8-17"
FILENAME="$FOLDER/occ.txt"
HUNDRED_MILLION=100000000
touch $FILENAME
echo "Concurrency Protocol: OCC\n" > $FILENAME
for THETA in 0.3 1.2
do 
    for NUM_WORKERS in 4 8 16 32 48 64
    do 
        for RUN in 1 2 3
        do
            printf "\nTheta : $THETA, Run : $RUN\n" >> $FILENAME
            ./micro_benchmark -a2 -sf10 -sf$THETA -t$HUNDRED_MILLION -c$NUM_WORKERS >> $FILENAME
        done
    done
done