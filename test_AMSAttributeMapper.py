import unittest
import mock
import logging
from mock import MagicMock,patch
import Toolkit.MetaClasses
import Toolkit.Config
import Toolkit.Exceptions
from Toolkit.Exceptions import AMSAttributeMapperException, AMSAttributeMapperInfoException
from Toolkit.Config import AMSAttributeMapper


class AMSAttributeMapperTests(unittest.TestCase):
	def setUp(self):
	# Force the singleton to be created each and every time a new test method is invoked
	    Toolkit.MetaClasses.Singleton._instances = {}

	def test_AMSAttributeMapperSingleton(self):
		mapper1 = Toolkit.Config.AMSAttributeMapper()
		mapper2 = Toolkit.Config.AMSAttributeMapper()
		self.assertTrue(mapper1 == mapper2)

	def test_AMSAttributeMapperValidSetAndGet(self):
		attribute = 'x'
		value = 'string'
		mapper = Toolkit.Config.AMSAttributeMapper()

		mapper.set_attribute(attribute, value)

		self.assertTrue(mapper.get_attribute(attribute) == value)

	#@mock.patch('Toolkit.Lib.AMSLogger.logger')
	def test_AMSAttributeMapperInvalidSet(self):
		attribute = 1
		value = 'string'
		mapper = Toolkit.Config.AMSAttributeMapper()

		with self.assertRaises(Toolkit.Exceptions.AMSAttributeMapperException):
			mapper.set_attribute(attribute, value)
		#mock_logger.log.assert_any_call(logging.CRITICAL,
										#MockFindValidator('attribute in set_attribute() must be a string'))



	@mock.patch.object(AMSAttributeMapper,'get_attribute')
	def test_AMSAttributeMapperValidSetWithEmpty(self,mock_get_attr):
		attribute = 'xyz'
		value = 'string'
		only_if_empty = True
		#mock_get_attr=MagicMock(side_effect=AMSAttributeMapperInfoException('attribute does not exist in attribute map.  Please set the attribute first via the set_attribute() method.'))
		#mock_get_attr.side_effect = AMSAttributeMapperInfoException
		mapper = Toolkit.Config.AMSAttributeMapper()
		mapper.set_attribute(attribute, value, only_if_empty)
		mock_get_attr.assert_called_once_with(attribute)
		self.assertEqual(mapper.set_attribute(attribute, value, only_if_empty), True)

	def test_AMSAttributeMapperValidGetDoesNotExist(self):
		attribute = 'xx'
		mapper = Toolkit.Config.AMSAttributeMapper()
		self.assertRaisesRegexp(AMSAttributeMapperInfoException,'attribute does not exist in attribute map.  Please set the attribute first via the set_attribute',mapper.get_attribute,attribute)

	def test_AMSAttributeMapperInvalidGetAttribute(self):
		attribute = 123
		mapper = Toolkit.Config.AMSAttributeMapper()
		self.assertRaisesRegexp(AMSAttributeMapperException,'attribute in get_attribute\(\) must be a string',mapper.get_attribute,attribute)



if __name__ == '__main__':
	unittest.main()
