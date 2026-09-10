import { onUnmounted } from 'vue'
import type { PipelineSnapshot, StatsSnapshot, WarehouseSnapshot } from './useDashboardStore'

export interface SnapshotSink {
  applyStats(stats: StatsSnapshot): void
  applyPipeline(pipeline: PipelineSnapshot): void
  applyWarehouses(warehouses: WarehouseSnapshot): void
}

/**
 * Polls the dashboard API for the authoritative counters. The server keeps these in
 * DashboardStats and they are correct regardless of how many SSE events reached the browser,
 * so a dropped burst or a stream reconnect costs a few lines in the live feed instead of
 * leaving every number on the page permanently wrong.
 */
export function useSnapshotPolling(sink: SnapshotSink, intervalMs = 1000) {
  let timer: ReturnType<typeof setInterval> | null = null
  let inFlight = false

  async function refresh() {
    if (inFlight) return
    inFlight = true
    try {
      const [stats, pipeline, warehouses] = await Promise.all([
        fetch('/api/dashboard/stats').then(r => r.json() as Promise<StatsSnapshot>),
        fetch('/api/dashboard/pipeline').then(r => r.json() as Promise<PipelineSnapshot>),
        fetch('/api/dashboard/warehouses').then(r => r.json() as Promise<WarehouseSnapshot>),
      ])
      sink.applyStats(stats)
      sink.applyPipeline(pipeline)
      sink.applyWarehouses(warehouses)
    } catch {
      // Backend unreachable (e.g. Vite dev without the API) — keep the last snapshot.
    } finally {
      inFlight = false
    }
  }

  function start() {
    if (timer !== null) return
    void refresh()
    timer = setInterval(() => { void refresh() }, intervalMs)
  }

  function stop() {
    if (timer !== null) {
      clearInterval(timer)
      timer = null
    }
  }

  onUnmounted(stop)

  return { refresh, start, stop }
}
