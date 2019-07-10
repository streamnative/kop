#!/usr/bin/env bash

# Set JAVA_HOME here to override the environment setting
# JAVA_HOME=

# Log4j configuration file
# KOP_LOG_CONF=

# Configuration file of settings used in Kop
# KOP_CONF=

# Extra options to be passed to the jvm
KOP_MEM=${KOP_MEM:-"-Xms2g -Xmx2g -XX:MaxDirectMemorySize=4g"}

# Garbage collection options
KOP_GC=" -XX:+UseG1GC -XX:MaxGCPauseMillis=10 -XX:+ParallelRefProcEnabled -XX:+UnlockExperimentalVMOptions -XX:+AggressiveOpts -XX:+DoEscapeAnalysis -XX:ParallelGCThreads=32 -XX:ConcGCThreads=32 -XX:G1NewSizePercent=50 -XX:+DisableExplicitGC -XX:-ResizePLAB"

# Extra options to be passed to the jvm
KOP_EXTRA_OPTS="${KOP_EXTRA_OPTS} ${KOP_MEM} ${KOP_GC} -Dio.netty.leakDetectionLevel=disabled -Dio.netty.recycler.maxCapacity.default=1000 -Dio.netty.recycler.linkCapacity=1024"

