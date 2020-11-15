import unittest

from unittest import TestCase
from decimal import Decimal

from pyspark.sql import types as T


class SparkTestCase(TestCase):
    """
    Extends base test case to deal with

    * Pyspark DataFrames equality
    """

    def __init__(self, methodName=None):
        """
        The base 'TestCase' by default raises a ValueError if the instance does
        not have a method with the specified name 'methodName'.
        We need to stop the base constructor call to prevent this behaviour.

        Note: No longer an issue in Python3
        """
        if methodName:
            unittest.TestCase.__init__(self, methodName)

    def assertListEqual(self, list1, list2, msg=None):
        """
        How can an experienced engineer live without lists?
        """
        sum_check = sum([1 for i, j in zip(list1, list2) if i == j])

        if any([
            len(list1) != len(list2),
            sum_check != len(list1)
        ]):
            raise self.failureException('Lists not equal %s: %s' % (list1, list2))

    def assertDataFrameEqual(self, df1, df2, verbose=True, msg=None):
        """
        Equality means both data (content) and metadata (column names, types, etc)
        """
        df1.cache()
        df2.cache()

        if df1.schema != df2.schema:
            raise self.failureException('DFs schemas are not equal %s : %s' % (df1.schema, df2.schema))

        if any([
            df1.subtract(df2).count() != 0,
            df2.subtract(df1).count() != 0,
        ]):
            df1.show(truncate=False)
            df2.show(truncate=False)

            if verbose:
                print("'{df1} \ {df2}'")
                df1.subtract(df2).show(truncate=False)
                print("'{df2} \ {df1}'")
                df2.subtract(df1).show(truncate=False)

            raise self.failureException('DFs values are not equal %s : %s' % (df1.collect(), df2.collect()))

    def assertDictAlmostEqual(self, d1, d2, places=7):
        """
        Asserts that two dictionaries have the same keys and values,
        with the values being compared up to specified precision
        """
        self.assertListEqual(sorted(d1.keys()), sorted(d2.keys()))

        def loop(l):
            if not l:
                return None

            i, j = l[0]
            key1, value1 = i
            key2, value2 = j

            if type(value1) != type(value2):
                raise self.failureException('Values are not equal %s : %s for key %s' % (value1, value2, key1))
            elif isinstance(value1, dict):
                self.assertDictAlmostEqual(value1, value2, places)
            elif isinstance(value1, Decimal):
                self.assertAlmostEqual(value1, value2, places)
            else:
                self.assertEqual(value1, value2)

            loop(l[1:])

        items = zip(sorted(d1.items()), sorted(d2.items()))

        loop(items)

    def assertDataFrameAlmostEqual(self, df1, df2, places=7):
        """
        Asserts the two dataframes are equal up to an acceptable precision
        Note: A dataframe is almost equal to another when their decimals differ up to a specified precision
        """

        df1.cache()
        df2.cache()

        if df1.schema != df2.schema:
            raise self.failureException('DFs schemas are not equal %s : %s' % (df1.schema, df2.schema))

        if any([
            df1.subtract(df2).count() != 0,
            df2.subtract(df1).count() != 0,
        ]):
            has_decimals = any([isinstance(f.dataType, T.DecimalType) for f in df1.schema.fields])
            if not has_decimals:
                df1.show()
                df2.show()
                raise self.failureException('DFs values are not equal %s : %s' % (df1.collect(), df2.collect()))

            for r1, r2 in zip(sorted(df1.collect()), sorted(df2.collect())):
                self.assertDictAlmostEqual(r1.asDict(), r2.asDict(), places)

