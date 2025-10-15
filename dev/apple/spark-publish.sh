#!/usr/bin/env bash

set -e

# Build Spark modules for publishing
./gradlew -DscalaVersion=2.13 :iceberg-spark:iceberg-spark-4.0_2.13:build :iceberg-spark:iceberg-spark-extensions-4.0_2.13:build :iceberg-spark:iceberg-spark-runtime-4.0_2.13:build

# Set up local maven repo
home_dir=$(pwd)

# Publish Spark modules to local repo
# Version is read from version.txt (matches apple-1.5.0.x pattern)
./gradlew -DscalaVersion=2.13 -Dmaven.repo.local=${home_dir}/local-repo :iceberg-spark:iceberg-spark-4.0_2.13:publishToMavenLocal :iceberg-spark:iceberg-spark-extensions-4.0_2.13:publishToMavenLocal :iceberg-spark:iceberg-spark-runtime-4.0_2.13:publishToMavenLocal

# Publish Hive Metastore (needed for shadow-tests classifier)
./gradlew -Dmaven.repo.local=${home_dir}/local-repo :iceberg-hive-metastore:publishToMavenLocal

# Stage Spark artifacts
ci stage-lib --many-many-artifacts "./local-repo/**/*.jar"
ci stage-lib --many-many-artifacts "./local-repo/**/*.pom"