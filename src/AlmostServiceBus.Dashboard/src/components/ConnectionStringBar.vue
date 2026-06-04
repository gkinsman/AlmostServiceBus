<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { api } from '../api/client'

const connectionString = ref('')
const copied = ref(false)

onMounted(async () => {
  try {
    connectionString.value = (await api.getInfo()).connectionString
  } catch {
    // The dashboard still works without the connection-string panel.
  }
})

async function copy() {
  if (!connectionString.value) return
  try {
    await navigator.clipboard.writeText(connectionString.value)
    copied.value = true
    setTimeout(() => { copied.value = false }, 1500)
  } catch {
    // Clipboard API is unavailable outside secure contexts; ignore.
  }
}
</script>

<template>
  <div v-if="connectionString" class="conn">
    <div class="conn-label">Connection string</div>
    <div class="conn-row">
      <code class="conn-value" :title="connectionString">{{ connectionString }}</code>
      <button
        class="conn-copy"
        :class="{ copied }"
        :title="copied ? 'Copied!' : 'Copy to clipboard'"
        @click="copy"
      >{{ copied ? '✓ Copied' : 'Copy' }}</button>
    </div>
  </div>
</template>

<style scoped>
.conn { border-top: 1px solid var(--dark-border); padding: 8px 12px; background: var(--dark); }
.conn-label { color: var(--dark-text-muted); font-size: 10px; text-transform: uppercase; font-weight: 700; letter-spacing: 0.8px; margin-bottom: 5px; }
.conn-row { display: flex; align-items: center; gap: 6px; }
.conn-value { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 10px; color: var(--dark-text); background: var(--dark-surface); border: 1px solid var(--dark-border); border-radius: 5px; padding: 5px 8px; }
.conn-copy { flex-shrink: 0; cursor: pointer; font-size: 10px; font-weight: 600; color: var(--dark-text); background: var(--dark-surface); border: 1px solid var(--dark-border); border-radius: 5px; padding: 5px 9px; white-space: nowrap; transition: all 0.12s; }
.conn-copy:hover { border-color: var(--blue); color: #fff; }
.conn-copy.copied { background: var(--green); border-color: var(--green); color: #fff; }
</style>
