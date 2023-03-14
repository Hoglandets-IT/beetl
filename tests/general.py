import unittest
from src.beetl import beetl

class TestMain(unittest.TestCase):
    def setUp(self):
        self.valuePairs = [
            {
                "src": [
                    {
                        "key": "KeyInBoth",
                        "value": "ValueInBoth"
                    },
                    {
                        "key": "KeyInSrc",
                        "value": "ValueInSrc"
                    },
                    {
                        "key": "KeyInBoth",
                        "value": "ValueInSrc"
                    },
                    {
                        "key": "KeyInSrc",
                        "value": "ValueInBoth"
                    }
                ],
                "dst": [
                    {
                        "key": "KeyInBoth",
                        "value": "ValueInBoth"
                    },
                    {
                        "key": "KeyInDst",
                        "value": "ValueInDst"
                    },
                    {
                        "key": "KeyInBoth",
                        "value": "ValueInDst"
                    },
                    {
                        "key": "KeyInDst",
                        "value": "ValueInBoth"
                    }
                ]
            }
        ]

    def test_main(self):
        comparer = beetl.Beetl(
            beetl.Source.from_json(self.valuePairs[0]["src"]),
            beetl.Destination.from_json(self.valuePairs[0]["dst"])
        )
        
        
        
        for item in self.values:
            tester = tpl_main.PythonTemplate(item["input"][0])
            self.assertEqual(
                tester.get_x_times(item["input"][1]),
                item["expect"]
            )

if __name__ == '__main__':
    unittest.main()