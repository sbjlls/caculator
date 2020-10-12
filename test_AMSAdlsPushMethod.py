import os
import sys
import shutil
import unittest
import logging
import mock


from Toolkit.Lib.RoutingMethods import AbstractAMSMethod,AMSADLSPushMethod
from Toolkit.Lib.Helpers import AMSADLS
from Toolkit.Exceptions import AMSMethodException, AMSFatalException, AMSADLSException
from Toolkit.Config import AMSFileRouteMethod, AMSJibbixOptions
from Toolkit.Tests import MockFindValidator

@mock.patch("azure.datalake.store.lib.auth",return_value='token')
@mock.patch("azure.datalake.store.core.AzureDLFileSystem")
@mock.patch("Toolkit.Config.AbstractAMSConfig.decrypt",return_value="secret_code")
@mock.patch("Toolkit.Config.AMSAttributeMapper.get_attribute",return_value="Zabbix")
class AMSAdlsPushMethodTests(unittest.TestCase):

    @mock.patch("azure.datalake.store.lib.auth", return_value='token')
    @mock.patch("azure.datalake.store.core.AzureDLFileSystem")
    @mock.patch("Toolkit.Config.AbstractAMSConfig.decrypt", return_value="secret_code")
    @mock.patch("Toolkit.Config.AMSAttributeMapper.get_attribute", return_value="Zabbix")
    def setUp(self,mock_get_attr,mock_decrypt,mock_client,mock_auth):
        global adlspush
        global config
        global jibbix_options
        config = AMSFileRouteMethod()
        jibbix_options = AMSJibbixOptions()
        adlspush = AMSADLSPushMethod(config, jibbix_options)

    def test_init(self,mock_get_attr,mock_decrypt,mock_client,mock_auth):
        config=AMSFileRouteMethod()
        jibbix_options=AMSJibbixOptions()
        adlspush = AMSADLSPushMethod(config,jibbix_options)
        self.assertIsInstance(adlspush.ams_adls,AMSADLS)
        mock_client.assert_called()

    @mock.patch("Toolkit.Lib.RoutingMethods.AMSADLSPushMethod._get_all_files_in_local_directory")
    @mock.patch("Toolkit.Lib.AMSLogger.logger")
    def test_get_file_list_dir_not_empty(self,mock_logger,mock_gafild,mock_get_attr,mock_decrypt,mock_client,mock_auth):
        config.from_directory="/path/from/dir"
        mock_gafild.return_value=['alpha','beta','sigma','delta']
        self.assertTrue(adlspush._get_file_list())
        self.assertListEqual(adlspush._found_files,mock_gafild.return_value)
        mock_logger.log.assert_any_call(logging.DEBUG,MockFindValidator("Found 4 files in /path/from/dir"))


    @mock.patch("Toolkit.Lib.RoutingMethods.AMSADLSPushMethod._get_all_files_in_local_directory")
    @mock.patch("Toolkit.Lib.AMSLogger.logger")
    def test_get_file_list_dir_empty(self, mock_logger, mock_gafild, mock_get_attr, mock_decrypt, mock_client, mock_auth):
        config.from_directory = "/path/from/dir"
        mock_gafild.return_value = []
        self.assertFalse(adlspush._get_file_list())
        mock_logger.log.assert_any_call(logging.DEBUG, MockFindValidator("Found 0 files in /path/from/dir"))

    @mock.patch("Toolkit.Lib.RoutingMethods.AMSADLSPushMethod._get_all_files_in_local_directory")
    def test_get_file_list_exception_raises(self,mock_gafild,mock_get_attr,mock_decrypt,mock_client,mock_auth):
        mock_gafild.side_effect=Exception("Exception caught for _get_file_list")
        with self.assertRaisesRegexp(AMSMethodException,"Exception caught for _get_file_list"):
            adlspush._get_file_list()

    @mock.patch("Toolkit.Lib.Helpers.AMSADLS.put")
    @mock.patch("Toolkit.Lib.AMSLogger.logger")
    def test_route_tmp_valid(self,mock_logger,mock_put,mock_get_attr,mock_decrypt,mock_client,mock_auth):
        config.from_directory="/path/from/dir"
        source_file="source_file.zip"
        to_path_tmp="/path/to/tmp"
        self.assertTrue(adlspush._route_tmp(source_file,to_path_tmp))
        mock_logger.log.assert_any_call(logging.DEBUG,MockFindValidator("Pushing file: source_file.zip to tmp folder: /path/to/tmp"))
        mock_put.assert_called_with("/path/from/dir/source_file.zip","/path/to/tmp")

    @mock.patch("Toolkit.Lib.Helpers.AMSADLS.put")
    @mock.patch("Toolkit.Lib.AMSLogger.logger")
    def test_route_tmp_fatal_exception(self, mock_logger,mock_put, mock_get_attr, mock_decrypt, mock_client, mock_auth):
        config.from_directory = "/path/from/dir"
        source_file = "source_file.zip"
        to_path_tmp = "/path/to/tmp"
        mock_put.side_effect = AMSFatalException("Pushing file failed!")
        with self.assertRaises(Exception):
            adlspush._route_tmp(source_file,to_path_tmp)
        mock_logger.log.assert_any_call(logging.DEBUG,MockFindValidator("Pushing file: source_file.zip to tmp folder: /path/to/tmp"))

    @mock.patch("Toolkit.Lib.Helpers.AMSADLS.put")
    @mock.patch("Toolkit.Lib.AMSLogger.logger")
    def test_route_tmp_exception_raises(self, mock_logger, mock_put, mock_get_attr, mock_decrypt, mock_client,
                                       mock_auth):
        config.from_directory = "/path/from/dir"
        source_file = "source_file.zip"
        to_path_tmp = "/path/to/tmp"
        mock_put.side_effect = Exception("Failed to push file to tmp folder")
        with self.assertRaisesRegexp(AMSMethodException,"Failed to push file to tmp folder"):
            adlspush._route_tmp(source_file,to_path_tmp)

    @mock.patch("Toolkit.Lib.Helpers.AMSADLS.rename")
    @mock.patch("Toolkit.Lib.AMSLogger.logger")
    def test_route_final_valid(self,mock_logger,mock_ren,mock_get_attr,mock_decrypt,mock_client,mock_auth):
        source_file="source_file"
        final_target="/path/final/target"
        to_path_tmp="/to/path/tmp"
        adlspush._route_final(source_file,final_target,to_path_tmp)
        mock_logger.log.assert_any_call(logging.DEBUG,MockFindValidator("Moving file from tmp folder: /to/path/tmp to final dest: /path/final/target"))
        mock_ren.assert_called_with(to_path_tmp,final_target)

    @mock.patch("Toolkit.Lib.Helpers.AMSADLS.rename")
    @mock.patch("Toolkit.Lib.AMSLogger.logger")
    def test_route_final_fatal_exception(self,mock_logger,mock_ren,mock_get_attr,mock_decrypt,mock_client,mock_auth):
        source_file = "source_file"
        final_target = "/path/final/target"
        to_path_tmp = "/to/path/tmp"
        mock_ren.side_effect=AMSFatalException("Fatal exception caught")
        with self.assertRaises(Exception):
            adlspush._route_final(source_file,final_target,to_path_tmp)

    @mock.patch("Toolkit.Lib.Helpers.AMSADLS.rename")
    @mock.patch("Toolkit.Lib.AMSLogger.logger")
    def test_route_final_methodexception(self, mock_logger, mock_ren, mock_get_attr, mock_decrypt, mock_client,
                                         mock_auth):
        source_file = "source_file"
        final_target = "/path/final/target"
        to_path_tmp = "/to/path/tmp"
        mock_ren.side_effect = AMSMethodException("AMSMethodException caught for _route_final")
        with self.assertRaisesRegexp(AMSMethodException,"AMSMethodException caught for _route_final"):
            adlspush._route_final(source_file, final_target, to_path_tmp)

    @mock.patch("Toolkit.Lib.Helpers.AMSADLS.stat")
    @mock.patch("Toolkit.Lib.AMSLogger.logger")
    def test_check_remote_final_target_valid(self,mock_logger,mock_stat, mock_get_attr, mock_decrypt, mock_client,mock_auth):
        final_target = "/path/to/final/target"
        self.assertIsNone(adlspush._check_remote_final_target(final_target))
        mock_logger.log.assert_any_call(logging.DEBUG,MockFindValidator("Checking remote for: /path/to/final/target"))
        mock_stat.assert_called_with(final_target)

    @mock.patch("Toolkit.Lib.Helpers.AMSADLS.stat")
    def test_check_remote_final_target_exception_raises(self,mock_stat, mock_get_attr, mock_decrypt, mock_client,mock_auth):
        mock_stat.side_effect=Exception("Exception caught for _check_remote_final_target")
        final_target = "/path/to/final/target"
        with self.assertRaisesRegexp(AMSFatalException,"Exception caught for _check_remote_final_target"):
            adlspush._check_remote_final_target(final_target)

    @mock.patch("Toolkit.Lib.Helpers.AMSADLS.create_dir")
    @mock.patch("Toolkit.Lib.AMSLogger.logger")
    def test_check_and_create_target_tmp_dir_valid(self,mock_logger,mock_mkdir,mock_get_attr, mock_decrypt, mock_client,mock_auth):
        adlspush._to_tmp_folder="/path/to/tmp/folder"
        rt = adlspush._check_and_create_target_tmp_dir()
        mock_logger.log.assert_any_call(logging.DEBUG,MockFindValidator("Creating tmp dir on remote: /path/to/tmp/folder"))
        mock_mkdir.assert_called_with(adlspush._to_tmp_folder)
        self.assertEqual(rt,mock_mkdir.return_value)

    @mock.patch("Toolkit.Lib.Helpers.AMSADLS.create_dir")
    def test_check_and_create_target_tmp_dir_exception_raises(self, mock_mkdir, mock_get_attr, mock_decrypt,
                                                   mock_client, mock_auth):
        adlspush._to_tmp_folder = "/path/to/tmp/folder"
        mock_mkdir.side_effect=Exception("Exception caught for _check_and_create_target_tmp_dir")
        with self.assertRaisesRegexp(AMSMethodException,"Exception caught for _check_and_create_target_tmp_dir"):
            adlspush._check_and_create_target_tmp_dir()

    @mock.patch("Toolkit.Lib.Helpers.AMSADLS.create_dir")
    @mock.patch("Toolkit.Lib.AMSLogger.logger")
    def test_check_and_create_target_to_dir_valid(self,mock_logger,mock_mkdir,mock_get_attr, mock_decrypt, mock_client,mock_auth):
        config.to_directory="/path/to/dir"
        rt=adlspush._check_and_create_target_to_dir()
        mock_logger.log.assert_any_call(logging.DEBUG,MockFindValidator("Creating target dir on remote: /path/to/dir"))
        mock_mkdir.assert_called_with(config.to_directory)
        self.assertEqual(rt,mock_mkdir.return_value)

    @mock.patch("Toolkit.Lib.Helpers.AMSADLS.create_dir")
    def test_check_and_create_target_to_dir_exception_raises(self, mock_mkdir, mock_get_attr, mock_decrypt,
                                                  mock_client, mock_auth):
        config.to_directory = "/path/to/dir"
        mock_mkdir.side_effect=Exception("Exception caught for _check_and_create_target_to_dir")
        with self.assertRaisesRegexp(AMSMethodException,"Exception caught for _check_and_create_target_to_dir"):
            adlspush._check_and_create_target_to_dir()

    @mock.patch("Toolkit.Lib.Helpers.AMSADLS.listdir")
    @mock.patch("Toolkit.Lib.AMSLogger.logger")
    def test_check_remote_dir_valid(self,mock_logger,mock_listdir,mock_get_attr, mock_decrypt,
                                                  mock_client, mock_auth):
        directory="/path/to/dir"
        self.assertTrue(adlspush._check_remote_dir(directory))
        mock_logger.log.assert_any_call(logging.DEBUG,MockFindValidator("Checking remote dir: /path/to/dir"))
        mock_listdir.assert_called_with(directory)

    @mock.patch("Toolkit.Lib.Helpers.AMSADLS.listdir")
    def test_check_remote_dir_exception_raises(self, mock_listdir, mock_get_attr, mock_decrypt,
                                    mock_client, mock_auth):
        directory = "/path/to/dir"
        mock_listdir.side_effect=Exception("Exception caught for _check_remote_dir")
        with self.assertRaisesRegexp(AMSMethodException,""):
            adlspush._check_remote_dir(directory)

    @mock.patch("Toolkit.Lib.Helpers.AMSADLS.create_dir")
    @mock.patch("Toolkit.Lib.AMSLogger.logger")
    def test_create_remote_dir_valid(self,mock_logger,mock_mkdir,mock_get_attr, mock_decrypt,
                                    mock_client, mock_auth):
        directory="/path/to/dir"
        adlspush._create_remote_dir(directory)
        mock_logger.log.assert_any_call(logging.DEBUG,MockFindValidator("Creating remote dir: /path/to/dir"))
        mock_mkdir.assert_called_with(directory)

    @mock.patch("Toolkit.Lib.Helpers.AMSADLS.create_dir")
    @mock.patch("Toolkit.Lib.AMSLogger.logger")
    def test_create_remote_dir_exception_raises(self,mock_logger,mock_mkdir, mock_get_attr, mock_decrypt,
                                     mock_client, mock_auth):
        directory = "/path/to/dir"
        mock_mkdir.side_effect = Exception("Could not create remote dir: /path/to/dir")
        with self.assertRaisesRegexp(AMSFatalException,"Could not create remote dir: /path/to/dir"):
            adlspush._create_remote_dir(directory)
        mock_logger.log.assert_any_call(logging.CRITICAL,MockFindValidator("Could not create remote dir: /path/to/dir"))

    @mock.patch("os.remove")
    @mock.patch("Toolkit.Lib.AMSLogger.logger")
    def test_remove_source_file_valid(self,mock_logger,mock_rem, mock_get_attr, mock_decrypt,
                                     mock_client, mock_auth):
        config.from_directory="/path/from/dir"
        source_file="source_file.zip"
        adlspush._remove_source_file(source_file)
        mock_logger.log.assert_any_call(logging.DEBUG,MockFindValidator("Removing source file: /path/from/dir/source_file.zip"))
        mock_rem.assert_called_with("/path/from/dir/source_file.zip")

    @mock.patch("os.remove")
    @mock.patch("Toolkit.Lib.AMSLogger.logger")
    def test_remove_source_file_exception_raises(self, mock_logger, mock_rem, mock_get_attr, mock_decrypt,
                                      mock_client, mock_auth):
        config.from_directory = "/path/from/dir"
        source_file = "source_file.zip"
        mock_rem.side_effect=Exception("File corrupted!")
        with self.assertRaisesRegexp(AMSFatalException,"File corrupted!"):
            adlspush._remove_source_file(source_file)
        mock_logger.log.assert_any_call(logging.DEBUG,MockFindValidator("Removing source file: /path/from/dir/source_file.zip"))
        mock_logger.log.assert_any_call(logging.CRITICAL,MockFindValidator("Could not remove source file: source_file.zip with error: File corrupted!"))

    @mock.patch("Toolkit.Lib.Helpers.AMSADLS.check_modified")
    @mock.patch("Toolkit.Lib.AMSLogger.logger")
    def test_check_modified_time_valid(self,mock_logger,mock_check,mock_get_attr, mock_decrypt,
                                      mock_client, mock_auth):
        file_path="/path/to/file"
        rt=adlspush._check_modified_time(file_path)
        mock_logger.log.assert_any_call(logging.DEBUG,MockFindValidator("Checking modified time for /path/to/file"))
        mock_check.assert_called_with(file_path)
        self.assertEqual(rt,mock_check.return_value)

    @mock.patch("Toolkit.Lib.Helpers.AMSADLS.check_modified")
    @mock.patch("Toolkit.Lib.AMSLogger.logger")
    def test_check_modified_time_exception(self,mock_logger,mock_check,mock_get_attr, mock_decrypt,
                                      mock_client, mock_auth):
        mock_check.side_effect=Exception("Exception caught for _check_modified_time")
        file_path = "/path/to/file"
        adlspush._check_modified_time(file_path)
        mock_logger.log.assert_any_call(logging.CRITICAL,MockFindValidator("Could not find modified time for file: /path/to/file with error: Exception caught for _check_modified_time"))
