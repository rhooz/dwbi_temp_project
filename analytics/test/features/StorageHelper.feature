@storagehelper
Feature: Verify functionality of StorageHelper

  Scenario: Create a new bucket
    Given I have a need to store files
    When I create a new bucket
    Then The new bucket is created

  Scenario: Deleting a non-existent file throws a predictable exception
    Given I have a non-existent file I try to delete
    When I delete the non-existent file
    Then The non-existent file throws a predictable exception

  Scenario: Small Files can be deleted
    Given I have a small file to delete
    When I delete the file
    Then The deleted file no longer exists

  Scenario: Big Files can be deleted
    Given I have a big file to delete
    When I delete the file
    Then The deleted file no longer exists

  Scenario: I have a multi-level directory that I am deleting
    Given I have a multi-level directory that I try to delete
    When I delete the multi-level directory
    Then The multi-level directory is gone

  @sc-1
  Scenario: Download a file
    Given I have a file to download
    When I pull down the file
    Then I can see the file locally

  Scenario: Delete a bucket
    Given I really want to clean things up
    When I delete a bucket
    Then The bucket is gone