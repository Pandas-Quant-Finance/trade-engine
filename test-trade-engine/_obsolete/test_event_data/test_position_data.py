from unittest import TestCase

import pandas as pd

from tradeengine._obsolete.events import *

# show all columns
pd.set_option('display.max_columns', None)


class TestPositionData(TestCase):

    def test_equal_position(self):
        self.assertEqual(Position(None, "AAPL", 10, 100), Position(None, "AAPL", -3, -100),)
        self.assertEqual(Position("12", "AAPL", 10, 100), Position("12", "AAPL", -3, -100),)
        self.assertNotEqual(Position("13", "AAPL", 10, 100), Position("12", "AAPL", -3, -100),)
        self.assertNotEqual(Position("13", "AAPL", 10, 100), Position("13", "MSFT", -3, -100),)
        self.assertIsNotNone(Position("13", "AAPL", 10, 100).__hash__())

    def test_long_position(self):
        self.assertEqual(100, (Position(None, "AAPL", 10, 100) + (-10, 110)).pnl)
        self.assertEqual(40, (Position(None, "AAPL", 10, 100) + (-4, 110)).pnl)
        self.assertEqual(120 + 40, ((Position(None, "AAPL", 10, 100) + (-4, 110)) + (-6, 120)).pnl)

        self.assertEqual(0, (Position(None, "AAPL", 10, 100) + (4, 110)).pnl)
        self.assertEqual(1440 / 14, (Position(None, "AAPL", 10, 100) + (4, 110)).cost_basis)
        self.assertEqual((1440 + 720) / 20, ((Position(None, "AAPL", 10, 100) + (4, 110)) + (6, 120)).cost_basis)

    def test_short_position(self):
        self.assertEqual(-100, (Position(None, "AAPL", -10, 100) + (10, 110)).pnl)
        self.assertEqual(-40, (Position(None, "AAPL", -10, 100) + (4, 110)).pnl)
        self.assertEqual(-40 + -120, ((Position(None, "AAPL", -10, 100) + (4, 110)) + (6, 120)).pnl)
        self.assertEqual(40 + 120, ((Position(None, "AAPL", -10, 120) + (4, 110)) + (6, 100)).pnl)

        self.assertEqual(0, (Position(None, "AAPL", -10, 100) + (-4, 110)).pnl)
        self.assertEqual(1440 / 14, (Position(None, "AAPL", -10, 100) + (-4, 110)).cost_basis)
        self.assertEqual((1440 + 720) / 20, ((Position(None, "AAPL", -10, 100) + (-4, 110)) + (-6, 120)).cost_basis)

    def test_swing_position(self):
        self.assertEqual(100, (Position(None, "AAPL", -10, 110) + (20, 100)).pnl)
        self.assertEqual(-100, (Position(None, "AAPL", 10, 110) + (-20, 100)).pnl)

        self.assertEqual(100 + 50, ((Position(None, "AAPL", -10, 110) + (20, 100)) + (-30, 105)).pnl)

    def test_trade_sequence(self):
        seq = []
        p = Position(None, "AAPL", 6, 100)
        seq.append(p)

        p += (4, 105)  # 10, 102
        seq.append(p)

        p -= (6, 110)  # 4, 102, 48, 0.78
        seq.append(p)

        p -= (6, 100)  # -2, 100, 48 - 8,
        seq.append(p)

        p -= (8, 102)  # -10, 101.6, 40,
        seq.append(p)

        p += (5, 101)  # -5, 101.6, 43
        seq.append(p)

        p += (5, 102)  # 0, 101.6, 41
        seq.append(p)

        p += (5, 104)  # 5, 1004, 41
        seq.append(p)

        #print("\n", "\n".join([str(p) for p in seq]))

        # test pnl
        self.assertListEqual(
            [0, 0, 48, 40, 40, 43, 41, 41],
            [p.pnl for p in seq]
        )

        # test cost basis
        self.assertListEqual(
            [100, 102.0, 102.0, 100, 101.6, 101.6, 101.6, 104],
            [p.cost_basis for p in seq]
        )

        # test realized and unrealized pnl
        ts = pd.DataFrame(p.evaluate(100) for p in seq)
        print(ts)
        self.assertListEqual(
            [0, 0, 48, 40, 40, 43, 41, 41],
            ts["realized_pnl"].tolist()
        )
        self.assertListEqual(
            [0, 20, 8, 0, -16, -8, 0, 20],
            ts["unrealized_pnl"].tolist()
        )
