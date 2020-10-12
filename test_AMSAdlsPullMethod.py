import os
import sys
import shutil
import unittest
import mock
import logging
from Toolkit.Lib.RoutingMethods import AbstractAMSMethod, AMSADLSPullMethod
from Toolkit.Lib.Helpers import AMSADLS
from Toolkit.Exceptions import AMSMethodException, AMSFatalException, AMSADLSException
from Toolkit.Config import AMSFileRouteMethod,AMSJibbixOptions
from azure.datalake.store import core, lib, multithread, exceptions
from Toolkit.Tests import MockFindValidator


@mock.patch("azure.datalake.store.lib.auth",return_value='token')
@mock.patch("azure.datalake.store.core.AzureDLFileSystem")
@mock.patch("Toolkit.Config.AbstractAMSConfig.decrypt",return_value="secret_code")
@mock.patch("Toolkit.Config.AMSAttributeMapper.get_attribute",return_value="Zabbix")
class AMSAdlsPullMethodTests(unittest.TestCase):

    @mock.patch("azure.datalake.store.lib.auth", return_value='token')
    @mock.patch("azure.datalake.store.core.AzureDLFileSystem")
    @mock.patch("Toolkit.Config.AbstractAMSConfig.decrypt", return_value="secret_code")
    @mock.patch("Toolkit.Config.AMSAttributeMapper.get_attribute", return_value="Zabbix")
    def setUp(self, mock_get_attr, mock_decrypt, mock_client, mock_auth):
        global adlspull
        global config
        global jibbix_options
        config = AMSFileRouteMethod()
        jibbix_options = AMSJibbixOptions()
        adlspull = AMSADLSPullMethod(config, jibbix_options)


    def test_init(self,mock_get_attr,mock_decrypt,mock_client,mock_auth):
        config=AMSFileRouteMethod()
        jibbix_options=AMSJibbixOptions()
        adlspull = AMSADLSPullMethod(config,jibbix_options)
        self.assertIsInstance(adlspull.ams_adls,AMSADLS)
        mock_client.assert_called()


    @mock.patch("Toolkit.Lib.AMSLogger.logger")
    @mock.patch("Toolkit.Lib.Helpers.AMSADLS.listdir")
    def test_get_file_list_dir_not_empty(self,mock_listdir,mock_logger,mock_get_attr,mock_decrypt,mock_client,mock_auth):
        config.from_directory="/path/to/from/dir"
        mock_listdir.return_value = ['alpha', 'beta', 'project.conf']
        self.assertTrue(adlspull._get_file_list())
        self.assertListEqual(adlspull._found_files,['alpha','beta','project.conf'])
        mock_logger.log.assert_any_call(logging.DEBUG,MockFindValidator("Found 3 files in /path/to/from/dir"))

    @mock.patch("Toolkit.Lib.AMSLogger.logger")
    @mock.patch("Toolkit.Lib.Helpers.AMSADLS.listdir")
    def test_get_file_list_dir_empty(self, mock_listdir, mock_logger, mock_get_attr, mock_decrypt, mock_client, mock_auth):
        config.from_directory = "/path/to/from/dir"
        mock_listdir.return_value = []
        self.assertFalse(adlspull._get_file_list())
        self.assertListEqual(adlspull._found_files, [])
        mock_logger.log.assert_any_call(logging.DEBUG, MockFindValidator("Found 0 files in /path/to/from/dir"))

    @mock.patch("Toolkit.Lib.AMSLogger.logger")
    @mock.patch("Toolkit.Lib.Helpers.AMSADLS.listdir")
    def test_get_file_list_exception_raises(self,mock_listdir,mock_logger,mock_get_attr,mock_decrypt,mock_client,mock_auth):
        mock_listdir.side_effect=Exception("Exceptions caught for _get_file_list")
        with self.assertRaises(AMSMethodException):
            adlspull._get_file_list()
        mock_logger.log.assert_any_call(logging.CRITICAL,MockFindValidator("Exceptions caught"))

    @mock.patch("Toolkit.Lib.Helpers.AMSADLS.get")
    @mock.patch("Toolkit.Lib.AMSLogger.logger")
    def test_route_tmp_valid(self,mock_logger,mock_get,mock_get_attr,mock_decrypt,mock_client,mock_auth):
        source_file="source_file"
        to_path_tmp="to_path_tmp"
        self.assertTrue(adlspull._route_tmp(source_file,to_path_tmp))
        mock_logger.log.assert_any_call(logging.DEBUG,MockFindValidator("Getting file: source_file to tmp_dir: to_path_tmp"))
        mock_get.assert_called_with(source_file,to_path_tmp)

    @mock.patch("Toolkit.Lib.Helpers.AMSADLS.get")
    @mock.patch("Toolkit.Lib.AMSLogger.logger")
    def test_route_tmp_exception_raises(self,mock_logger,mock_get,mock_get_attr,mock_decrypt,mock_client,mock_auth):
        mock_get.side_effect=Exception("Exception caught for _route_tmp")
        with self.assertRaises(AMSMethodException):
            adlspull._route_tmp("source_file","to_path_tmp")
        mock_logger.log.assert_any_call(logging.CRITICAL,MockFindValidator("Exception caught for _route_tmp"))

    @mock.patch("shutil.copy")
    @mock.patch("Toolkit.Lib.AMSLogger.logger")
    def test_route_final_valid(self,mock_logger,mock_copy,mock_get_attr,mock_decrypt,mock_client,mock_auth):
        self.assertTrue(adlspull._route_final("source_file","target_dir","to_path_tmp"))
        mock_logger.log.assert_any_call(logging.DEBUG,MockFindValidator("Moving file from tmp_dir: to_path_tmp to final location: target_dir"))
        mock_copy.assert_called_with("to_path_tmp","target_dir")

    @mock.patch("shutil.copy")
    @mock.patch("Toolkit.Lib.AMSLogger.logger")
    def test_route_final_exception_raises(self,mock_logger,mock_copy,mock_get_attr,mock_decrypt,mock_client,mock_auth):
        mock_copy.side_effect=Exception("Exception caught for _route_final")
        with self.assertRaises(AMSMethodException):
            adlspull._route_final("source_file","target_dir","to_path_tmp")
        mock_logger.log.assert_any_call(logging.CRITICAL,MockFindValidator("Exception caught for _route_final"))

    @mock.patch("Toolkit.Lib.RoutingMethods.AMSADLSPullMethod._method_make_dir")
    @mock.patch("Toolkit.Lib.AMSLogger.logger")
    def test_check_and_create_target_to_dir_valid(self,mock_logger,mock_mkdir,mock_get_attr,mock_decrypt,mock_client,mock_auth):
        config.to_directory="to_directory"
        rt=adlspull._check_and_create_target_to_dir()
        self.assertEqual(rt,mock_mkdir.return_value)
        mock_logger.log.assert_any_call(logging.DEBUG,MockFindValidator("Check/Create target dir: to_directory"))
        mock_mkdir.assert_called_with("to_directory")

    @mock.patch("Toolkit.Lib.RoutingMethods.AMSADLSPullMethod._method_make_dir")
    def test_check_and_create_target_to_dir_exception_raises(self, mock_mkdir, mock_get_attr, mock_decrypt, mock_client,mock_auth):
        mock_mkdir.side_effect=Exception("Exception caught for _check_and_create_target_to_dir")
        config.to_directory = "to_directory"
        with self.assertRaisesRegexp(AMSMethodException,"Exception caught for _check_and_create_target_to_dir"):
            adlspull._check_and_create_target_to_dir()

    @mock.patch("Toolkit.Lib.RoutingMethods.AMSADLSPullMethod._method_make_dir")
    @mock.patch("Toolkit.Lib.AMSLogger.logger")
    def test_check_and_create_target_tmp_dir_valid(self,mock_logger,mock_mkdir,mock_get_attr, mock_decrypt, mock_client,mock_auth):
        adlspull._to_tmp_folder="/path/to/tmp/folder"
        rt = adlspull._check_and_create_target_tmp_dir()
        self.assertEqual(rt,mock_mkdir.return_value)
        mock_logger.log.assert_any_call(logging.DEBUG,MockFindValidator("Check/Create tmp_target dir: /path/to/tmp/folder"))
        mock_mkdir.assert_called_with("/path/to/tmp/folder")

    @mock.patch("Toolkit.Lib.RoutingMethods.AMSADLSPullMethod._method_make_dir")
    @mock.patch("Toolkit.Lib.AMSLogger.logger")
    def test_check_and_create_target_tmp_dir_exception_raises(self, mock_logger, mock_mkdir, mock_get_attr, mock_decrypt,
                                                   mock_client, mock_auth):
        mock_mkdir.side_effect=Exception("Exception caught for _check_and_create_target_tmp_dir")
        with self.assertRaisesRegexp(AMSMethodException,"Exception caught for _check_and_create_target_tmp_dir"):
            adlspull._check_and_create_target_tmp_dir()

    @mock.patch("os.path.exists")
    @mock.patch("Toolkit.Lib.AMSLogger.logger")
    def test_check_remote_final_target_valid(self,mock_logger,mock_exist,mock_get_attr, mock_decrypt,
                                                   mock_client, mock_auth):
        final_target="/path/to/final/target"
        rt=adlspull._check_remote_final_target(final_target)
        self.assertEqual(rt,mock_exist.return_value)
        mock_logger.log.assert_any_call(logging.DEBUG,MockFindValidator("Checking final target: /path/to/final/target"))
        mock_exist.assert_called_with(final_target)

    @mock.patch("os.path.exists")
    @mock.patch("Toolkit.Lib.AMSLogger.logger")
    def test_check_remote_final_target_ioerror(self, mock_logger, mock_exist, mock_get_attr, mock_decrypt,
                                             mock_client, mock_auth):
        final_target = "/path/to/final/target"
        mock_exist.side_effect=IOError
        self.assertFalse(adlspull._check_remote_final_target(final_target))
        mock_logger.log.assert_any_call(logging.DEBUG,MockFindValidator("Checking final target: /path/to/final/target"))

    @mock.patch("os.path.exists")
    @mock.patch("Toolkit.Lib.AMSLogger.logger")
    def test_check_remote_final_target_exception_raises(self, mock_logger, mock_exist, mock_get_attr, mock_decrypt,
                                               mock_client, mock_auth):
        final_target = "/path/to/final/target"
        mock_exist.side_effect=Exception("Exception caught for _check_remote_final_target")
        with self.assertRaisesRegexp(AMSFatalException,"Exception caught for _check_remote_final_target"):
            adlspull._check_remote_final_target(final_target)
        mock_logger.log.assert_any_call(logging.DEBUG,MockFindValidator("Checking final target: /path/to/final/target"))

    @mock.patch("Toolkit.Lib.Helpers.AMSADLS.remove_file")
    @mock.patch("Toolkit.Lib.AMSLogger.logger")
    def test_remove_source_file_valid(self,mock_logger,mock_remove,mock_get_attr, mock_decrypt,
                                               mock_client, mock_auth):
        source_file="/path/source_files"
        adlspull._remove_source_file(source_file)
        # self.assertEqual(abs_path_source_file,source_file)
        mock_logger.log.assert_any_call(logging.DEBUG,MockFindValidator("Removing source file: /path/source_files"))
        mock_remove.assert_called_with("/path/source_files")

    @mock.patch("Toolkit.Lib.Helpers.AMSADLS.remove_file")
    @mock.patch("Toolkit.Lib.AMSLogger.logger")
    def test_remove_source_file_exception_raises(self, mock_logger, mock_remove, mock_get_attr, mock_decrypt,
                                      mock_client, mock_auth):
        mock_remove.side_effect=AMSADLSException("AMSADLSException caught for _remove_source_file")
        source_file = "/path/source_files"
        with self.assertRaises(Exception):
            adlspull._remove_source_file(source_file)
        mock_logger.log.assert_any_call(logging.DEBUG, MockFindValidator("Removing source file: /path/source_files"))

    @mock.patch("Toolkit.Lib.Helpers.AMSADLS.remove_file")
    @mock.patch("Toolkit.Lib.AMSLogger.logger")
    def test_remove_source_file_fatal_exception_raises(self, mock_logger, mock_remove, mock_get_attr, mock_decrypt,
                                                 mock_client, mock_auth):
        mock_remove.side_effect = Exception("Exception caught for _remove_source_file")
        source_file = "/path/source_files"
        with self.assertRaisesRegexp(AMSFatalException,"Exception caught for _remove_source_file"):
            adlspull._remove_source_file(source_file)
        mock_logger.log.assert_any_call(logging.DEBUG, MockFindValidator("Removing source file: /path/source_files"))
        mock_logger.log.assert_any_call(logging.CRITICAL,MockFindValidator("Could not remove source file: /path/source_files with error: Exception caught for _remove_source_file"))

    @mock.patch("Toolkit.Lib.Helpers.AMSADLS.check_modified")
    @mock.patch("Toolkit.Lib.AMSLogger.logger")
    def test_check_modified_time_valid(self,mock_logger,mock_check,mock_get_attr, mock_decrypt,
                                                 mock_client, mock_auth):
        file_path="/path/to/file"
        rt=adlspull._check_modified_time(file_path)
        mock_logger.log.assert_any_call(logging.DEBUG,MockFindValidator("Checking modified time for /path/to/file"))
        mock_check.assert_called_with(file_path)
        self.assertEqual(rt,mock_check.return_value)

    @mock.patch("Toolkit.Lib.Helpers.AMSADLS.check_modified")
    @mock.patch("Toolkit.Lib.AMSLogger.logger")
    def test_check_modified_time_exception_raises(self, mock_logger, mock_check, mock_get_attr, mock_decrypt,
                                       mock_client, mock_auth):
        mock_check.side_effect=Exception("Exception caught for _check_modified_time")
        file_path = "/path/to/file"
        adlspull._check_modified_time(file_path)
        mock_logger.log.assert_any_call(logging.DEBUG, MockFindValidator("Checking modified time for /path/to/file"))
        mock_logger.log.assert_any_call(logging.CRITICAL,MockFindValidator("Could not find modified time for file: /path/to/file with error: Exception caught for _check_modified_time"))
