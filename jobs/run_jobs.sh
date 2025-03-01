#!/bin/bash
# Script to compile, run, and display output for selected TransAnalysis jobs

set -e  # Exit on error

HADOOP_CP=$(hadoop classpath)
TRANS_INPUT=/user/hien2706/input/trans.txt

# Function to run a single job
run_job() {
    JOB_NAME=$1
    OUTPUT_DIR="/user/hien2706/output${JOB_NAME: -1}"  # Extracts the last digit (output1, output2, ...)
    JAR_FILE="transanalysis${JOB_NAME: -1}.jar"

    echo "Compiling $JOB_NAME..."
    javac -classpath "$HADOOP_CP" ${JOB_NAME}.java
    jar cf $JAR_FILE ${JOB_NAME}*.class

    echo "Removing old output directory ($OUTPUT_DIR)..."
    hdfs dfs -rm -r $OUTPUT_DIR || true

    echo "Running $JOB_NAME..."
    hadoop jar $JAR_FILE $JOB_NAME $TRANS_INPUT $OUTPUT_DIR

    echo "===== Results for $JOB_NAME ====="
    hdfs dfs -cat ${OUTPUT_DIR}/part-* || echo "No output found"
    echo -e "\n==================================\n"
}

# Check if arguments are provided
if [ "$#" -eq 0 ]; then
    echo "Usage: ./run_jobs.sh [TransAnalysis1] [TransAnalysis2] ... [all]"
    echo "Example: ./run_jobs.sh TransAnalysis1 TransAnalysis3 TransAnalysis5"
    exit 1
fi

# Run selected jobs
for job in "$@"; do
    if [ "$job" == "all" ]; then
        for i in {1..6}; do
            run_job "TransAnalysis$i"
        done
        break
    elif [[ "$job" =~ ^TransAnalysis[1-6]$ ]]; then
        run_job "$job"
    else
        echo "Invalid job name: $job"
    fi
done
