# NYC Taxi Dataset Analysis using Apache Spark on Google Cloud Platform (GCP)

This repository contains the code, documentation, and Jupyter Notebook files for our final assignment, which involves the analysis of the NYC Taxi dataset using Apache Spark deployed on Google Cloud Platform (GCP).

## Overview

In this assignment, we aim to demonstrate our proficiency in the following skills:

1. Deploying a Spark cluster on Google Cloud Platform.
2. Utilizing the Spark DataFrame API for data manipulation and analysis.
3. Organizing code into well-structured Spark jobs.
4. Extracting meaningful business insights using Spark.

## Team Members

- Sameera holy Sheik abdullah
- Mugil Raja RamamoorthyKolanchi

## Project Structure

The repository is organized as follows:

- `data/`: This directory contains the NYC Taxi dataset (not included in the repository due to size constraints). You can download the dataset from [link] and place it here.
- `src/`: This directory contains the Spark jobs and related code.
    - `jobs/`: Each analysis section has a corresponding Spark job implemented here.
        - `trip_analysis`: Spark job for analyzing trip-related data.
        - `tip_analysis`: Spark job for analyzing tip-related data.
        - `fare_analysis`: Spark job for analyzing fare-related data.
        - `demand_prediction`: Spark job for predicting taxi demand.
- `notebooks/`: This directory contains Jupyter Notebook files for each analysis section.
    - `NYC_spark.ipynb`: Jupyter Notebook for code analysis.
- `README.md`: This document, providing an overview of the project, its structure, and instructions for reproducing the results.

## Deployment on GCP

To deploy a Spark cluster on Google Cloud Platform, follow these steps:

1. Set up a GCP project and enable necessary APIs.
2. Create a Dataproc cluster with Spark.
3. Upload the dataset to a storage bucket.
4. Submit Spark jobs using the `gcloud` command-line tool.

## Running the Analysis

1. Clone this repository to your local machine.
2. Set up the GCP project and configure the necessary credentials.
3. Adjust the cluster and job configurations in the Spark job scripts.
4. Run the Spark jobs using the provided scripts in the `src/jobs` directory.
5. Explore the Jupyter Notebook file in the directory for detailed analysis and visualizations.

## Reporting(note)

We have included detailed comments within the Spark job scripts and Jupyter Notebook files to explain our analysis approach and interpretation of results. You can find these explanations within each file.

## Analysis Sections

### Trip Analysis

In the `trip_analysis` section, we explore and analyze various aspects related to taxi trips, such as trip duration, distance, and pickup/drop-off locations. We delve into patterns and trends to uncover insights about taxi usage within NYC.

### Tip Analysis

The `tip_analysis` section focuses on analyzing tip-related data. We examine factors that influence tip amounts, such as trip distance, time of day, and location, to provide a deeper understanding of tipping behavior.

### Fare Analysis

In the `fare_analysis` section, we analyze fare-related information. This includes studying fare amounts in relation to trip attributes and identifying any anomalies or noteworthy trends in the fare data.

### Demand Prediction

The `demand_prediction` section involves predicting taxi demand based on historical data. We build a predictive model using Spark to anticipate high-demand areas and times, which can provide valuable insights for taxi service optimization.

