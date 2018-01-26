from behave import *
import os
import sys
from behave import *

class_path = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../../lib'))
data_path = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../data'))
sys.path.append(class_path)

use_step_matcher("re")

# Scenario: Create a new bucket
@given("I have a need to store files")
def step_impl(context):
    context.bucketToCreate = "behave-testing-bucket"
    pass

@when("I create a new bucket")
def step_impl(context):
    context.gs.createBucket(context.bucketToCreate)
    pass

@then("The new bucket is created")
def step_impl(context):
    ret = context.gs.checkExist(context.bucketToCreate)
    assert ret

# Scenario: Deleting a non-existent file throws a predictable exception
@given("I have a non-existent file I try to delete")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    context.fileToUpload = "ThisFileDoesNotExist"
    pass

@when("I delete the non-existent file")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    try:
        context.gs.deleteFile(context.fileToUpload)
    except Exception as gsErr:
        context.NotFoundException = gsErr
    pass

@then("The non-existent file throws a predictable exception")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    print("print exception message")
    print(context.NotFoundException)
    assert "Not Found" in str(context.NotFoundException)

# Small Files can be deleted
@given("I have a small file to delete")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    context.filetouse = "1-t_cnfg_module_dmsn.csv"
    context.fileToUpload = data_path + '/' + context.filetouse
    context.gs.uploadFile(context.fileToUpload, context.filetouse)
    pass

@when("I delete the file")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    context.gs.deleteFile(context.filetouse)
    pass

@then("The deleted file no longer exists")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    fileExists = context.gs.checkFileExists(context.filetouse)
    assert not fileExists

# Scenario: Big Files can be deleted
@given("I have a big file to delete")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    context.filetouse = "1-t_src_ship_to_loc-BIG.csv"
    context.fileToUpload = data_path + '/' + context.filetouse
    context.gs.uploadFile(context.fileToUpload, context.filetouse)
    pass

# Scenario: I have a multi-level directory that I am deleting
@given("I have a multi-level directory that I try to delete")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    context.filetouse =  "1-t_cnfg_module_dmsn.csv"
    context.dir = 'level1/level2/'

    context.fileToUpload = data_path + '/' + context.filetouse
    context.gs.uploadFile(context.fileToUpload,  context.dir + '/' + context.filetouse)
    pass

@when("I delete the multi-level directory")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    context.gs.deleteFolder(context.dir)
    pass

@then("The multi-level directory is gone")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    assert not context.gs.checkFileExists(context.dir + '/' + context.filetouse)
    assert not context.gs.checkFileExists(context.dir)

# Scenario: Download a file

@given("I have a file to download")
def step_impl(context):
    context.filetouse = "1-t_cnfg_module_dmsn.csv"
    context.fileToUpload = data_path + '/' + context.filetouse
    context.gs.uploadFile(context.fileToUpload, context.filetouse)

@when("I pull down the file")
def step_impl(context):
    context.gs.downloadFile(context.filetouse, 'the-downloaded-version.csv')

@then("I can see the file locally")
def step_impl(context):
    assert os.path.isfile('the-downloaded-version.csv')
    os.remove('the-downloaded-version.csv')

# Scenario: Delete a bucket
@given("I really want to clean things up")
def step_impl(context):
    context.bucketToDelete = "behave-testing-bucket"
    assert context.gs.checkExist(context.bucketToDelete)
    pass

@when("I delete a bucket")
def step_impl(context):
    context.gs.deleteBucket(context.bucketToDelete)
    pass

@then("The bucket is gone")
def step_impl(context):
    assert not context.gs.checkExist(context.bucketToDelete)
