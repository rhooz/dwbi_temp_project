@salesforce
Feature: Support data interactions with salesforce

  Scenario: Pull object from salesforce into a csv file
    Given there is a salesforce object
     When a salesforce export call is initiated
     Then a csv file containing the object is produced

  Scenario: Place pulled object into database
    Given there is a salesforce csv
     When a sync call is initiated
     Then a table containing the data from the csv is produced