import unittest

from spark3.utils import hash_unsafe_bytes


class UtilsTest(unittest.TestCase):
    def test_hash_unsafe_bytes(self):
        self.assertEqual(
            hash_unsafe_bytes('0x34be5b8c30ee4fde069dc878989686abe9884470', 42),
            -190531000
        )

        self.assertEqual(
            hash_unsafe_bytes('0x67ed645ec1994c60d7e3664ca2bed0a56b48595f', 42),
            -1058307760
        )

        self.assertEqual(
            hash_unsafe_bytes('0x833b94afa97b7e763a86a3e83dcaf58603857371', 42),
            881193080
        )
