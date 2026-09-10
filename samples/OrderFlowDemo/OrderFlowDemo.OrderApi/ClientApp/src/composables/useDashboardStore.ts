import { reactive, computed } from 'vue'
import type { DashboardEvent } from './useSse'

export interface FeedItem {
  id: string
  type: string
  orderId?: string
  state?: string
  warehouse?: string
  failureReason?: string
  timestamp: string
}

/** Shape of GET /api/dashboard/stats */
export interface StatsSnapshot {
  total: number
  completed: number
  failed: number
  inFlight: number
}

/** Shape of GET /api/dashboard/pipeline (only states with count > 0 are returned) */
export type PipelineSnapshot = { state: string; count: number }[]

/** Shape of GET /api/dashboard/warehouses */
export type WarehouseSnapshot = { warehouseId: string; depth: number }[]

const MAX_FEED_ITEMS = 50
const THROUGHPUT_WINDOW_SECONDS = 60
/** How often buffered SSE events are applied to reactive state. */
const RENDER_INTERVAL_MS = 250

const state = reactive({
  // Authoritative numbers come from polling the API, not from counting events. Counting
  // events client-side drifted permanently whenever a burst dropped or the stream reconnected.
  pipelineCounts: {} as Record<string, number>,
  warehouseDepths: {} as Record<string, number>,
  totalOrders: 0,
  completedOrders: 0,
  failedOrders: 0,
  inFlightOrders: 0,
  // Live feed and throughput are the only things derived from the SSE stream.
  feedItems: [] as FeedItem[],
  /** Events per second for the last 60 seconds, oldest first. */
  throughputBuckets: new Array<number>(THROUGHPUT_WINDOW_SECONDS).fill(0),
})

let eventCounter = 0

// Ring buffer of per-second counts keyed by epoch second. O(1) per event, regardless of rate.
// The previous implementation kept one entry per event and rebuilt the whole array on every
// event, which at Black Friday rates (~450 events/s) meant copying tens of thousands of entries
// hundreds of times a second and froze the tab.
const ring = new Array<number>(THROUGHPUT_WINDOW_SECONDS).fill(0)
const ringSeconds = new Array<number>(THROUGHPUT_WINDOW_SECONDS).fill(-1)

function bumpThroughput(nowMs: number) {
  const second = Math.floor(nowMs / 1000)
  const slot = second % THROUGHPUT_WINDOW_SECONDS
  if (ringSeconds[slot] !== second) {
    ringSeconds[slot] = second
    ring[slot] = 0
  }
  ring[slot]++
}

function snapshotThroughput(nowMs: number): number[] {
  const nowSecond = Math.floor(nowMs / 1000)
  const out = new Array<number>(THROUGHPUT_WINDOW_SECONDS)
  for (let i = 0; i < THROUGHPUT_WINDOW_SECONDS; i++) {
    const second = nowSecond - (THROUGHPUT_WINDOW_SECONDS - 1 - i)
    const slot = second % THROUGHPUT_WINDOW_SECONDS
    out[i] = ringSeconds[slot] === second ? ring[slot] : 0
  }
  return out
}

// Incoming events are buffered and applied on a timer so rendering cost is bounded by the
// timer, not by the event rate. Every component (including two Chart.js charts) re-renders
// on each reactive flush, so one flush per event was the other half of the freeze.
let pending: DashboardEvent[] = []
let flushTimer: ReturnType<typeof setInterval> | null = null

function flushPending() {
  const now = Date.now()
  if (pending.length > 0) {
    const batch = pending
    pending = []

    const newItems: FeedItem[] = []
    for (const event of batch) {
      if (event.type !== 'saga-transition' || !event.toState) continue
      bumpThroughput(new Date(event.timestamp).getTime() || now)
      newItems.push({
        id: `${++eventCounter}`,
        type: event.type,
        orderId: event.orderId,
        state: event.toState,
        warehouse: event.warehouse ?? undefined,
        failureReason: event.failureReason ?? undefined,
        timestamp: event.timestamp,
      })
    }

    if (newItems.length > 0) {
      // Newest first; only the last MAX_FEED_ITEMS of the batch can be visible anyway.
      newItems.reverse()
      state.feedItems = newItems.slice(0, MAX_FEED_ITEMS)
        .concat(state.feedItems)
        .slice(0, MAX_FEED_ITEMS)
    }
  }

  // Refresh the chart even when idle so the window keeps sliding to the right.
  state.throughputBuckets = snapshotThroughput(now)
}

function ensureFlushTimer() {
  if (flushTimer === null) {
    flushTimer = setInterval(flushPending, RENDER_INTERVAL_MS)
  }
}

export function useDashboardStore() {
  ensureFlushTimer()

  /** Queue an SSE event; it is applied to state on the next render tick. */
  function processEvent(event: DashboardEvent) {
    pending.push(event)
  }

  function applyStats(stats: StatsSnapshot) {
    state.totalOrders = stats.total
    state.completedOrders = stats.completed
    state.failedOrders = stats.failed
    state.inFlightOrders = stats.inFlight
  }

  function applyPipeline(pipeline: PipelineSnapshot) {
    // The API omits zero-count states, so replace the map rather than merging into it.
    const counts: Record<string, number> = {}
    for (const entry of pipeline) counts[entry.state] = entry.count
    state.pipelineCounts = counts
  }

  function applyWarehouses(warehouses: WarehouseSnapshot) {
    const depths: Record<string, number> = {}
    for (const entry of warehouses) depths[entry.warehouseId] = entry.depth
    state.warehouseDepths = depths
  }

  const inFlight = computed(() => state.inFlightOrders)

  const throughputPerSecond = computed(() => {
    const total = state.throughputBuckets.reduce((sum, n) => sum + n, 0)
    return Math.round(total / THROUGHPUT_WINDOW_SECONDS * 10) / 10
  })

  return {
    state,
    processEvent,
    applyStats,
    applyPipeline,
    applyWarehouses,
    inFlight,
    throughputPerSecond,
  }
}
