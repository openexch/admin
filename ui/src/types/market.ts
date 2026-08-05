// SPDX-License-Identifier: Apache-2.0
// Minimal market catalog carried from trading-ui for the Risk tab. RiskAdmin
// maps a marketId to its symbol; the full trade-app market types are not
// needed here.
export interface Market {
  id: number;
  symbol: string;
  baseAsset: string;
  quoteAsset: string;
  name: string;
  /** Engine price grid (match MarketConfig): orders must land on a tick
   *  inside [minPrice, maxPrice] or the engine rejects them. */
  tickSize: number;
  minPrice: number;
  maxPrice: number;
}

export const MARKETS: Market[] = [
  { id: 1, symbol: 'BTC-USD', baseAsset: 'BTC', quoteAsset: 'USD', name: 'Bitcoin', tickSize: 1, minPrice: 50_000, maxPrice: 150_000 },
  { id: 2, symbol: 'ETH-USD', baseAsset: 'ETH', quoteAsset: 'USD', name: 'Ethereum', tickSize: 0.5, minPrice: 1_000, maxPrice: 10_000 },
  { id: 3, symbol: 'SOL-USD', baseAsset: 'SOL', quoteAsset: 'USD', name: 'Solana', tickSize: 0.05, minPrice: 50, maxPrice: 500 },
  { id: 4, symbol: 'XRP-USD', baseAsset: 'XRP', quoteAsset: 'USD', name: 'Ripple', tickSize: 0.001, minPrice: 0.5, maxPrice: 10 },
  { id: 5, symbol: 'DOGE-USD', baseAsset: 'DOGE', quoteAsset: 'USD', name: 'Dogecoin', tickSize: 0.0001, minPrice: 0.05, maxPrice: 1 },
];
