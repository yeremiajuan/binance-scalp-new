# PAPER | SYNTHETIC ledger reconciliation

State `/home/user/binance-scalp-new/evidence/demo_state.sqlite` · account `paper-synthetic-demo` · config `3f9aba57ef97c6da` · input `8b92caa29bbf6c93` · metadata SYNTHETIC `d1f5ef2bbeb9007e`

Assumptions from the stored configuration: buy fee 0.0010 (BTC), sell fee 0.0010 (USDT), slippage 0.0001 per side, tick 0.01. Spread is paid by crossing bid/ask and is not charged again.

Start: 1000 USDT, 0 BTC.

| # | side | qty | quote bid/ask | recomputed price | limit | fee (native) | USDT delta | BTC delta | running USDT | running BTC | check |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | BUY entry | 0.00292 | 61505.12/61507.12 | 61513.28 | 61532.04 | 0.000002920 BTC | -179.6187776 | 0.002917080 | 820.3812224 | 0.002917080 | OK |
| 2 | SELL exit | 0.00291 | 62135.12/62137.12 | 62128.90 | 61818.65 | 0.18079509900 USDT | 180.61430390100 | -0.00291 | 1000.99552630100 | 0.000007080 | OK |
| 3 | BUY entry | 0.002 | 62509.5/62511.5 | 62517.76 | 62532.75 | 0.0000020 BTC | -125.03552 | 0.0019980 | 875.96000630100 | 0.002005080 | OK |
| 4 | SELL exit | 0.001 | 63159.67/63161.67 | 63153.35 | 62830.6 | 0.063153350 USDT | 63.090196650 | -0.001 | 939.05020295100 | 0.001005080 | OK |
| 5 | SELL exit | 0.001 | 63173/63175 | 63166.68 | 62843.88 | 0.063166680 USDT | 63.103513320 | -0.001 | 1002.15371627100 | 0.000005080 | OK |
| 6 | BUY entry | 0.00287 | 63502.15/63504.15 | 63510.51 | 63532.9 | 0.000002870 BTC | -182.2751637 | 0.002867130 | 819.87855257100 | 0.002872210 | OK |
| 7 | SELL exit | 0.00287 | 63500.15/63502.15 | 63493.79 | 63179.67 | 0.18222717730 USDT | 182.04495012270 | -0.00287 | 1001.92350269370 | 0.000002210 | OK |
| 8 | BUY entry | 0.00288 | 63646.9/63648.9 | 63655.27 | 63682.72 | 0.000002880 BTC | -183.3271776 | 0.002877120 | 818.59632509370 | 0.002879330 | OK |
| 9 | SELL exit | 0.00287 | 63215.32/63217.32 | 63208.99 | 62899.58 | 0.18140980130 USDT | 181.22839149870 | -0.00287 | 999.82471659240 | 0.000009330 | OK |
| 10 | BUY entry | 0.0027 | 63693.2/63695.2 | 63701.57 | 63723.04 | 0.00000270 BTC | -171.994239 | 0.00269730 | 827.83047759240 | 0.002706630 | OK |
| 11 | SELL exit | 0.0027 | 63835.15/63837.15 | 63828.76 | 63512.99 | 0.1723376520 USDT | 172.1653143480 | -0.0027 | 999.99579194040 | 0.000006630 | OK |
| 12 | BUY entry | 0.00283 | 63962.15/63964.15 | 63970.55 | 63993.13 | 0.000002830 BTC | -181.0366565 | 0.002827170 | 818.95913544040 | 0.002833800 | OK |

## Balances and invariants

- Final USDT 818.9591354404 (free 818.9591354404, locked 0); final BTC 0.0028338 (free 0.0028338, locked 0).
- Fees by native asset: {'BTC': '0.000016200', 'USDT': '0.84308975960'}.
- Realized net P&L 0.418555051891184074; inventory basis 181.459419611491184074 for 0.002833800 BTC.
- [x] recomputed USDT == stored USDT free+locked
- [x] recomputed BTC == stored BTC free+locked
- [x] ledger sums == stored balances (free, locked)
- [x] BTC balance == inventory pool (tradable + dust)
- [x] no negative balance
- [x] realized net recomputed == stored
- [x] entry basis == USDT paid for buys (entry fee attributed once)
- [x] remaining basis == buy basis - sold basis
- [x] cash == start - buy cash + sell proceeds

## PROJECT_PLAN.md hand-check

Buy 0.01 BTC @ 50,000 with a 0.1% BTC fee, sell 0.00999 BTC @ 50,100 with a 0.1% USDT fee: final cash 999.99850100 USDT, BTC 0, net -0.00149900 -> OK

RESULT: RECONCILED
