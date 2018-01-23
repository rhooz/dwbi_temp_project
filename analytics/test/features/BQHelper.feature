@bigquery
Feature: Support data interactions with bigquery

  Scenario: Load data from a file into bigquery
    Given there is a table definition with a file
     When a request is placed to upload the file
     Then the file data is present in bigquery

  Scenario: Load data from GCS into bigquery
    Given there is a table definition with a GCS file
     When a request is made to load from GCS
     Then the GCS file data is present in bigquery

  Scenario: Place data from bigquery into a file
    Given there is a table in bigquery
     When a request to download table into a file
      And a request to download table into GCS
     Then the file is present locally
      And the file is present in GCS
