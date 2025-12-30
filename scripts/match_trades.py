"""
FINAL FIXED: Compare backtest vs live trades with proper date alignment.
All bugs fixed - ready to use.
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np

sys.path.append(str(Path(__file__).parent.parent))

from config import settings


def main():
    """Main comparison function."""
    print("\n" + "="*60)
    print(" BACKTEST VS LIVE COMPARISON (FINAL)")
    print("="*60)
    
    # Load trades
    # print("\n Loading trade data...")
    
    # if not settings.LIVE_TRADES_FILE.exists():
    #     print(f"❌ Live trades not found: {settings.LIVE_TRADES_FILE}")
    #     return
    
    # if not settings.BACKTEST_TRADES_FILE.exists():
    #     print(f"❌ Backtest trades not found: {settings.BACKTEST_TRADES_FILE}")
    #     return
    
    live_df = pd.read_csv(settings.LIVE_TRADES_FILE)
    backtest_df = pd.read_csv(settings.BACKTEST_TRADES_FILE)
    
    live_df['timestamp'] = pd.to_datetime(live_df['timestamp'])
    backtest_df['timestamp'] = pd.to_datetime(backtest_df['timestamp'])
    
    live_df['date'] = live_df['timestamp'].dt.date
    backtest_df['date'] = backtest_df['timestamp'].dt.date
    
    # Show structure
    print(f"\n LIVE TRADES:")
    print(f"   Total: {len(live_df)} records")
    print(f"   BUY: {len(live_df[live_df['side']=='BUY'])}")
    print(f"   SELL: {len(live_df[live_df['side']=='SELL'])}")
    print(f"   Time: {live_df['timestamp'].min()} to {live_df['timestamp'].max()}")
    print(f"   Dates: {sorted(live_df['date'].unique())}")
    
    print(f"\n BACKTEST TRADES:")
    print(f"   Total: {len(backtest_df)} records")
    print(f"   BUY: {len(backtest_df[backtest_df['side']=='BUY'])}")
    print(f"   SELL: {len(backtest_df[backtest_df['side']=='SELL'])}")
    print(f"   Time: {backtest_df['timestamp'].min()} to {backtest_df['timestamp'].max()}")
    print(f"   Dates: {sorted(backtest_df['date'].unique())}")
    
    # Compare BUY orders by date
    print(f"\n{'='*60}")
    print(f" COMPARING BUY ORDERS BY DATE")
    print(f"{'='*60}")
    
    live_buys = live_df[live_df['side'] == 'BUY'].copy()
    backtest_buys = backtest_df[backtest_df['side'] == 'BUY'].copy()
    
    live_dates = sorted(live_buys['date'].unique())
    backtest_dates = sorted(backtest_buys['date'].unique())
    
    print(f"\nLive dates: {live_dates}")
    print(f"Backtest dates: {backtest_dates}")
    
    missing = set(live_dates) - set(backtest_dates)
    extra = set(backtest_dates) - set(live_dates)
    
    if missing:
        print(f"  Backtest MISSING: {sorted(missing)}")
    if extra:
        print(f"  Backtest EXTRA: {sorted(extra)} (extra data other than the live trade period)")
    
    # Match date by date
    all_matches = []
    all_unmatched_live = []
    all_unmatched_backtest = []
    
    for date in live_dates:
        print(f"\n{date}")
        print(f"{'─'*60}")
        
        live_day = live_buys[live_buys['date'] == date].reset_index(drop=True)
        backtest_day = backtest_buys[backtest_buys['date'] == date].reset_index(drop=True)
        
        print(f"   Live BUY orders: {len(live_day)}")
        print(f"   Backtest BUY orders: {len(backtest_day)}")
        
        if len(backtest_day) == 0:
            print(f"    No backtest trades for this date!")
            all_unmatched_live.extend(live_day.to_dict('records'))
            continue
        
        # Match trades within this day
        matches, unmatched_live, unmatched_backtest = match_day(
            live_day, backtest_day, date
        )
        
        all_matches.extend(matches)
        all_unmatched_live.extend(unmatched_live)
        all_unmatched_backtest.extend(unmatched_backtest)
        
        print(f"   Matched: {len(matches)}")
        print(f"   Unmatched live: {len(unmatched_live)}")
    
    # Summary
    print(f"\n{'='*60}")
    print(f" OVERALL RESULTS")
    print(f"{'='*60}")
    
    total_live = len(live_buys)
    total_matched = len(all_matches)
    match_pct = (total_matched / total_live * 100) if total_live > 0 else 0
    
    print(f"\nTotal live BUY orders: {total_live}")
    print(f" Matched: {total_matched}")
    print(f" Unmatched live: {len(all_unmatched_live)}")
    print(f" Unmatched backtest: {len(all_unmatched_backtest)}")
    print(f"\n Match Rate: {match_pct:.1f}%")
    
    if match_pct < 50:
        print(f"\n  LOW MATCH RATE!")

    # Show sample matches
    if len(all_matches) > 0:
        print(f"\n{'='*60}")
        print(f" SAMPLE MATCHED TRADES")
        print(f"{'='*60}")
        
        for match in all_matches[:10]:
            print(f"\n   {match['date']}:")
            print(f"      Live:     {match['live_time'].strftime('%H:%M:%S')} @ ${match['live_entry']:.2f}")
            print(f"      Backtest: {match['backtest_time'].strftime('%H:%M:%S')} @ ${match['backtest_entry']:.2f}")
            print(f"      Time Difference : {match['time_diff_seconds']:.0f}s |  Price Difference: {match['entry_diff_pct']:.3f}%")
    
    # Show unmatched live trades
    if len(all_unmatched_live) > 0:
        print(f"\n{'='*60}")
        print(f" SAMPLE UNMATCHED LIVE TRADES")
        print(f"{'='*60}")
        
        for i, trade in enumerate(all_unmatched_live[:10]):
            ts = pd.to_datetime(trade['timestamp'])
            print(f"   #{i+1}: {ts.strftime('%Y-%m-%d %H:%M:%S')} @ ${trade['entry_price']:.2f}")
    
    # print(f"\n{'='*60}")
    # print(f" COMPARISON COMPLETE")
    # print(f"{'='*60}")


def match_day(live_day: pd.DataFrame, backtest_day: pd.DataFrame, date):
    """Match BUY orders within the same day."""
    matches = []
    unmatched_live = []
    matched_backtest = set()
    
    tolerance_minutes = 15
    
    for idx, live_order in live_day.iterrows():
        live_time = live_order['timestamp']
        live_entry = live_order['entry_price']
        
        best_match = None
        best_score = float('inf')
        
        # Find best matching backtest order
        for bt_idx, bt_order in backtest_day.iterrows():
            # Skip if already matched
            if bt_idx in matched_backtest:
                continue
            
            bt_time = bt_order['timestamp']
            bt_entry = bt_order['entry_price']
            
            # Check time difference
            time_diff_seconds = abs((live_time - bt_time).total_seconds())
            time_diff_minutes = time_diff_seconds / 60
            
            if time_diff_minutes > tolerance_minutes:
                continue
            
            # Check price difference
            price_diff_pct = abs(live_entry - bt_entry) / live_entry * 100
            
            # Combined score (weighted)
            score = time_diff_seconds + (price_diff_pct * 100)
            
            if score < best_score:
                best_score = score
                best_match = {
                    'idx': bt_idx,
                    'order': bt_order,
                    'time_diff_seconds': time_diff_seconds,
                    'entry_diff_pct': price_diff_pct
                }
        
        if best_match:
            # Record match
            match_info = {
                'date': date,
                'live_time': live_time,
                'backtest_time': best_match['order']['timestamp'],
                'time_diff_seconds': best_match['time_diff_seconds'],
                'live_entry': live_entry,
                'backtest_entry': best_match['order']['entry_price'],
                'entry_diff_pct': best_match['entry_diff_pct'],
            }
            matches.append(match_info)
            matched_backtest.add(best_match['idx'])
        else:
            # No match found
            unmatched_live.append(live_order.to_dict())
    
    # Get unmatched backtest orders
    unmatched_backtest = []
    for idx, bt_order in backtest_day.iterrows():
        if idx not in matched_backtest:
            unmatched_backtest.append(bt_order.to_dict())
    
    return matches, unmatched_live, unmatched_backtest


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"\n Comparison failed: {e}")
        import traceback
        traceback.print_exc()