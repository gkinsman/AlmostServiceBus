<script setup lang="ts">
import { useSse } from './composables/useSse'
import { useDashboardStore } from './composables/useDashboardStore'
import { useSnapshotPolling } from './composables/useSnapshotPolling'
import ScenarioBar from './components/ScenarioBar.vue'
import MetricCards from './components/MetricCards.vue'
import PipelineFlow from './components/PipelineFlow.vue'
import ThroughputChart from './components/ThroughputChart.vue'
import SagaDonut from './components/SagaDonut.vue'
import QueueDepths from './components/QueueDepths.vue'
import WarehouseFifo from './components/WarehouseFifo.vue'
import LiveFeed from './components/LiveFeed.vue'

const store = useDashboardStore()
const { state, processEvent, inFlight, throughputPerSecond } = store

// Counters and pipeline state are polled from the API (authoritative); the SSE stream only
// drives the live feed and the throughput chart, so a dropped burst can't skew the numbers.
const polling = useSnapshotPolling(store)
polling.start()

const { connected, onEvent, onOpen } = useSse('/api/dashboard/events')
onEvent(processEvent)
onOpen(() => { void polling.refresh() })
</script>

<template>
  <div class="app-shell">
    <div class="app-right">
      <header class="top-bar">
        <div class="top-bar-left">
          <h1 class="page-title">Dashboard</h1>
          <span class="sse-badge" :class="{ live: connected }">
            {{ connected ? '● SSE LIVE' : '○ DISCONNECTED' }}
          </span>
        </div>
        <ScenarioBar />
      </header>

      <main class="main-content">
        <!-- Metrics row -->
        <MetricCards
          :total="state.totalOrders"
          :completed="state.completedOrders"
          :failed="state.failedOrders"
          :in-flight="inFlight"
          :throughput="throughputPerSecond"
        />

        <!-- 2:1 grid: Pipeline + Sagas -->
        <div class="grid-2-1">
          <div class="card">
            <div class="card-title">Order Pipeline <span class="card-badge">LIVE</span></div>
            <PipelineFlow :counts="state.pipelineCounts" />
          </div>
          <div class="card">
            <div class="card-title">Saga States</div>
            <SagaDonut :counts="state.pipelineCounts" :in-flight="inFlight" />
          </div>
        </div>

        <!-- Throughput chart full width -->
        <div class="card">
          <div class="card-title">Throughput — 60 s window <span class="card-badge">STREAMING</span></div>
          <ThroughputChart :buckets="state.throughputBuckets" />
        </div>

        <!-- Bottom 3-col grid -->
        <div class="grid-3">
          <div class="card">
            <div class="card-title">Queue Depths</div>
            <QueueDepths :counts="state.pipelineCounts" />
          </div>
          <div class="card">
            <div class="card-title">Warehouse FIFO Lanes</div>
            <WarehouseFifo :depths="state.warehouseDepths" />
          </div>
          <div class="card">
            <div class="card-title">Live Event Feed <span class="card-badge">STREAMING</span></div>
            <LiveFeed :items="state.feedItems" />
          </div>
        </div>
      </main>
    </div>
  </div>
</template>
