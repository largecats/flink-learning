#!/usr/bin/env bash

set -e

#root="/mnt/c/Users/${USER}/Fun/programming/streaming-notes/flink/"
root="/home/datadev/${USER}/"

repoName="flink-learning"
moduleName=$(basename $(dirname $(dirname $(realpath ${BASH_SOURCE}))))
modelName=$(basename $(dirname $(realpath ${BASH_SOURCE})))

param=${@}
#flinkRun="/usr/local/flink-1.12.2/bin/flink run"
#flinkRun="/usr/local/flink-1.13.1_2.11/bin/flink run -m yarn-cluster"
flinkRun="/usr/local/flink-1.13.1_2.11/bin/flink run -t yarn-per-job"
target="target/scala-2.11/${modelName}.jar"

logDir="${root}/logs/${repoName}/${moduleName}_${modelName}"
mkdir -p ${logDir}
#logPath="${logDir}/${param}.log"
logPath="${logDir}/.log"
logPath=${logPath// /.}

cd `dirname $0` # move to directory where this shell script is in
${flinkRun} \
    ${target} ${param} \
    |& tee ${logPath}

flinkRunExit=${PIPESTATUS[0]}
exit ${flinkRunExit}