<script setup lang="ts">
import { computed } from 'vue'
import { Doughnut } from 'vue-chartjs'
import {
  Chart as ChartJS,
  ArcElement,
  Tooltip,
  Legend,
} from 'chart.js'
import type { ComputedRef } from 'vue'

ChartJS.register(ArcElement, Tooltip, Legend)

const props = defineProps<{
  counts: Record<string, number>
  inFlight: ComputedRef<number> | number
}>()

const stages = [
  { key: 'PaymentPending', label: 'Payment', color: '#f59e0b' },
  { key: 'InventoryReserving', label: 'Inventory', color: '#8b5cf6' },
  { key: 'Picking', label: 'Picking', color: '#3b82f6' },
  { key: 'Shipping', label: 'Shipping', color: '#068d9d' },
  { key: 'Shipped', label: 'Shipped', color: '#14b8a6' },
  { key: 'Delivered', label: 'Delivered', color: '#10b981' },
  { key: 'Invoiced', label: 'Invoiced', color: '#6b7280' },
  { key: 'PaymentFailed', label: 'Pmt Failed', color: '#c11f1f' },
  { key: 'BackOrdered', label: 'BackOrder', color: '#f97316' },
]

const chartData = computed(() => ({
  labels: stages.map(s => s.label),
  datasets: [{
    data: stages.map(s => props.counts[s.key] || 0),
    backgroundColor: stages.map(s => s.color),
    borderWidth: 1,
  }],
}))

const options = {
  responsive: true,
  maintainAspectRatio: false,
  plugins: {
    legend: { position: 'right' as const, labels: { font: { size: 11 } } },
  },
  cutout: '65%',
  // Counts refresh once a second from the API; animating every change is wasted work under load.
  animation: false as const,
}
</script>

<template>
  <div class="chart-container">
    <Doughnut :data="chartData" :options="options" />
  </div>
</template>
