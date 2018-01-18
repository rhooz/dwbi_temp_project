@services
Feature: Calling endpoints in the Processor Service

  Scenario: Run an ETL flow from a SQL source
    Given I have email object source data
     When I execute a call to transform that data
     Then A table is created with those results
