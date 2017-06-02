FILENAME="results_16w.txt"
printf "Experiments with 16 workers for varying theta" > $FILENAME
for THETA in 0.1 0.3 0.5 0.7 0.9 1.2 
do 
    printf "\n\nTheta : $THETA\n\n" >> $FILENAME
    for RUN in 1 2 3 4 5
    do 
        printf "\nTheta : $THETA, Run : $RUN\n" >> $FILENAME
        ./micro_benchmark -a2 -sf10 -sf$THETA -t10000000 -c16 >> $FILENAME
    done
done