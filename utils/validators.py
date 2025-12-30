

import pandas as pd
from datetime import timedelta
from typing import Tuple, Dict
from utils.logger import get_logger

logger = get_logger(__name__)


class TradeValidator:
    """
    FIXED Trade Validator.
    Matches trades by INTENT (signal timing + direction),
    not by fragile execution artifacts.
    """

    def __init__(
        self,
        candle_seconds: int = 180,   
        price_tolerance: float = 0.05
    ):
        """
        Args:
            candle_seconds: Candle duration in seconds (primary timeframe)
            price_tolerance: Soft tolerance for reporting only
        """
        self.candle_window = timedelta(seconds=candle_seconds)
        self.price_tolerance = price_tolerance

        logger.info(
            f"TradeValidator initialized: candle_window={candle_seconds}s, "
            f"price_tol={price_tolerance*100:.1f}%"
        )

    def load_trades(
        self,
        backtest_file: str,
        live_file: str
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:

        df_bt = pd.read_csv(backtest_file)
        df_lv = pd.read_csv(live_file)

        for df in (df_bt, df_lv):
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['side'] = df['side'].str.upper()

        df_bt = df_bt.sort_values('timestamp').reset_index(drop=True)
        df_lv = df_lv.sort_values('timestamp').reset_index(drop=True)

        logger.info(
            f"Loaded {len(df_bt)} backtest trades, {len(df_lv)} live trades"
        )

        return df_bt, df_lv

    # ------------------------------------------------------------------
    # Core Matching Logic
    # ------------------------------------------------------------------
    def compare_trades(
        self,
        df_bt: pd.DataFrame,
        df_lv: pd.DataFrame
    ) -> Dict:

        results = {
            "backtest_trades": len(df_bt),
            "live_trades": len(df_lv),
            "matched_trades": 0,
            "unmatched_backtest": 0,
            "unmatched_live": 0,
            "details": []
        }

        used_live = set()

        for bt_idx, bt in df_bt.iterrows():
            bt_time = bt['timestamp']
            bt_side = bt['side']
            bt_price = bt.get('entry_price')

            found = False

            for lv_idx, lv in df_lv.iterrows():
                if lv_idx in used_live:
                    continue

                # 1️⃣ Direction must match
                if bt_side != lv['side']:
                    continue

                # 2️⃣ Must be in same candle window
                time_diff = abs(lv['timestamp'] - bt_time)
                if time_diff > self.candle_window:
                    continue

                #  MATCH FOUND
                used_live.add(lv_idx)
                found = True
                results["matched_trades"] += 1

                price_diff_pct = None
                if bt_price and 'entry_price' in lv:
                    price_diff_pct = abs(bt_price - lv['entry_price']) / bt_price

                results["details"].append({
                    "status": "MATCHED",
                    "side": bt_side,
                    "bt_time": bt_time,
                    "lv_time": lv['timestamp'],
                    "time_diff_sec": time_diff.total_seconds(),
                    "bt_price": bt_price,
                    "lv_price": lv.get('entry_price'),
                    "price_diff_pct": (
                        price_diff_pct * 100 if price_diff_pct is not None else None
                    )
                })
                break

            if not found:
                results["unmatched_backtest"] += 1
                results["details"].append({
                    "status": "UNMATCHED_BACKTEST",
                    "side": bt_side,
                    "bt_time": bt_time,
                    "bt_price": bt_price
                })

        results["unmatched_live"] = len(df_lv) - len(used_live)

        logger.info(
            f"Trade comparison complete — "
            f"matched={results['matched_trades']}, "
            f"unmatched_bt={results['unmatched_backtest']}, "
            f"unmatched_lv={results['unmatched_live']}"
        )

        return results

    # ------------------------------------------------------------------
    # Report
    # ------------------------------------------------------------------
    def generate_report(self, results: Dict) -> str:
        lines = []
        lines.append("=" * 60)
        lines.append("TRADE MATCHING REPORT")
        lines.append("=" * 60)

        bt = results["backtest_trades"]
        lv = results["live_trades"]
        mt = results["matched_trades"]

        lines.append(f"Backtest Trades : {bt}")
        lines.append(f"Live Trades     : {lv}")
        lines.append(f"Matched Trades  : {mt}")

        if bt > 0:
            lines.append(f"Match Rate      : {mt / bt * 100:.2f}%")

        lines.append(f"Unmatched Backtest : {results['unmatched_backtest']}")
        lines.append(f"Unmatched Live     : {results['unmatched_live']}")

        lines.append("\n--- SAMPLE MATCH DETAILS (first 10) ---")

        for d in results["details"][:10]:
            if d["status"] == "MATCHED":
                lines.append(
                    f"MATCH {d['side']} | "
                    f"BT {d['bt_time']} ↔ LV {d['lv_time']} | "
                    f"Δt={d['time_diff_sec']:.0f}s"
                )
                if d["price_diff_pct"] is not None:
                    lines.append(
                        f"   Price diff: {d['price_diff_pct']:.2f}%"
                    )
            else:
                lines.append(
                    f"UNMATCHED BT {d['side']} @ {d['bt_time']}"
                )

        lines.append("=" * 60)
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def validate(
        self,
        backtest_file: str,
        live_file: str
    ) -> Tuple[Dict, str]:

        df_bt, df_lv = self.load_trades(backtest_file, live_file)
        results = self.compare_trades(df_bt, df_lv)
        report = self.generate_report(results)
        return results, report
