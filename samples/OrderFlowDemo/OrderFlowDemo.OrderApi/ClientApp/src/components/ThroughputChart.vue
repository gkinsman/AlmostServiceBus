<script setup lang="ts">
import { computed } from 'vue'
import { Line } from 'vue-chartjs'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Filler,
  Tooltip,
} from 'chart.js'

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Filler, Tooltip)

const props = defineProps<{
  /** Events per second for the last 60 seconds, oldest first (see useDashboardStore). */
  buckets: number[]
}>()

const labels = Array.from({ length: 60 }, (_, i) => i === 59 ? 'now' : `${59 - i}s`)

const chartData = computed(() => ({
  labels,
  datasets: [{
    label: 'Events/sec',
    data: props.buckets,
    fill: true,
    borderColor: '#068d9d',
    backgroundColor: 'rgba(6,141,157,0.2)',
    tension: 0.3,
    pointRadius: 0,
  }],
}))

const options = {
  responsive: true,
  maintainAspectRatio: false,
  // The data is refreshed on a fixed tick; animating between ticks just burns main-thread time.
  animation: false as const,
  plugins: { legend: { display: false } },
  scales: {
    x: { display: false },
    y: { beginAtZero: true, ticks: { precision: 0 } },
  },
}
</script>

<template>
  <div class="chart-container">
    <Line :data="chartData" :options="options" />
  </div>
</template>
