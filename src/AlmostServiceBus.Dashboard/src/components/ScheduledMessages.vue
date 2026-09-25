<script setup lang="ts">
import { ref, shallowRef, watch, onMounted, onUnmounted } from 'vue'
import type { MessageInfo, ScheduledMessageInfo } from '../types'
import { api } from '../api/client'
import { Play, Pencil, X, Check, FastForward } from 'lucide-vue-next'

/**
 * Scheduled messages for a namespace, or for one queue/topic when `entity` is set.
 * Every action is scoped to that namespace: rescheduling, shifting, delivering or cancelling
 * never touches another namespace's messages.
 */
const props = defineProps<{
  namespace: string
  entity?: string | null
}>()

const selectedMessage = defineModel<MessageInfo | null>('selectedMessage')

const items = shallowRef<ScheduledMessageInfo[]>([])
const loaded = ref(false)
const error = ref<string | null>(null)
const busy = ref(false)
const now = ref(Date.now())

/** Sequence number of the row whose time is being edited, and the draft value. */
const editing = ref<number | null>(null)
const draft = ref('')

const shiftAmount = ref(1)
const shiftUnit = ref<'s' | 'm' | 'h' | 'd'>('h')
const unitSeconds = { s: 1, m: 60, h: 3600, d: 86400 } as const

async function refresh() {
  try {
    items.value = await api.getScheduled(props.namespace, props.entity)
    error.value = null
  } catch (e) {
    error.value = (e as Error).message
  } finally {
    loaded.value = true
  }
}

/** Runs an admin action, then reloads so the list reflects what the broker now holds. */
async function act(action: () => Promise<unknown>) {
  busy.value = true
  try {
    await action()
    error.value = null
  } catch (e) {
    error.value = (e as Error).message
  } finally {
    busy.value = false
    await refresh()
  }
}

// Scheduled messages don't raise SSE events until they're delivered, so poll.
let pollTimer: ReturnType<typeof setInterval> | undefined
let clockTimer: ReturnType<typeof setInterval> | undefined
onMounted(() => {
  pollTimer = setInterval(refresh, 2000)
  clockTimer = setInterval(() => { now.value = Date.now() }, 1000)
})
onUnmounted(() => {
  clearInterval(pollTimer)
  clearInterval(clockTimer)
})

watch(() => [props.namespace, props.entity], () => {
  items.value = []
  loaded.value = false
  editing.value = null
  refresh()
}, { immediate: true })

function countdown(iso: string | null): string {
  if (!iso) return 'due'
  let secs = Math.round((new Date(iso).getTime() - now.value) / 1000)
  if (secs <= 0) return 'due'
  const d = Math.floor(secs / 86400); secs %= 86400
  const h = Math.floor(secs / 3600); secs %= 3600
  const m = Math.floor(secs / 60); const s = secs % 60
  if (d) return `in ${d}d ${h}h`
  if (h) return `in ${h}h ${m}m`
  if (m) return `in ${m}m ${s}s`
  return `in ${s}s`
}

/** ISO → the local "YYYY-MM-DDTHH:mm:ss" string a datetime-local input expects. */
function toLocalInput(iso: string | null): string {
  const d = iso ? new Date(iso) : new Date()
  const pad = (n: number) => String(n).padStart(2, '0')
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
}

function startEdit(item: ScheduledMessageInfo) {
  editing.value = item.message.sequenceNumber
  draft.value = toLocalInput(item.scheduledEnqueueTimeUtc)
}

function saveEdit(item: ScheduledMessageInfo) {
  const when = new Date(draft.value)
  if (Number.isNaN(when.getTime())) { error.value = 'Invalid date'; return }
  editing.value = null
  act(() => api.reschedule(props.namespace, item.message.sequenceNumber, when.toISOString()))
}

function deliverNow(item: ScheduledMessageInfo) {
  act(() => api.deliverScheduledNow(props.namespace, item.message.sequenceNumber))
  if (selectedMessage.value?.sequenceNumber === item.message.sequenceNumber) selectedMessage.value = null
}

function cancel(item: ScheduledMessageInfo) {
  if (!confirm(`Cancel scheduled message ${item.message.messageId}? It will never be delivered.`)) return
  act(() => api.cancelScheduled(props.namespace, item.message.sequenceNumber))
  if (selectedMessage.value?.sequenceNumber === item.message.sequenceNumber) selectedMessage.value = null
}

function shift(direction: 1 | -1) {
  const seconds = direction * shiftAmount.value * unitSeconds[shiftUnit.value]
  if (!Number.isFinite(seconds) || seconds === 0) return
  act(() => api.shiftScheduled(props.namespace, seconds, props.entity))
}

function deliverAll() {
  const scope = props.entity ? `for ${props.entity}` : `in namespace "${props.namespace}"`
  if (!confirm(`Deliver all ${items.value.length} scheduled message(s) ${scope} now?`)) return
  act(() => api.deliverAllScheduledNow(props.namespace, props.entity))
  selectedMessage.value = null
}
</script>

<template>
  <div class="scheduled">
    <div class="toolbar">
      <div class="shift" title="Move every scheduled message in this view earlier or later">
        <span class="label">Shift all</span>
        <button class="ghost" :disabled="busy || items.length === 0" @click="shift(-1)">−</button>
        <input v-model.number="shiftAmount" type="number" min="1" class="amount" />
        <select v-model="shiftUnit">
          <option value="s">sec</option>
          <option value="m">min</option>
          <option value="h">hr</option>
          <option value="d">day</option>
        </select>
        <button class="ghost" :disabled="busy || items.length === 0" @click="shift(1)">+</button>
      </div>
      <button class="primary" :disabled="busy || items.length === 0" @click="deliverAll">
        <FastForward :size="11" />Deliver all now
      </button>
    </div>

    <div v-if="error" class="error">{{ error }}</div>

    <div class="rows">
      <div
        v-for="item in items" :key="item.message.sequenceNumber"
        class="row"
        :class="{ selected: selectedMessage?.sequenceNumber === item.message.sequenceNumber }"
        @click="selectedMessage = item.message"
      >
        <div class="header">
          <span class="msg-id">{{ item.message.messageId.substring(0, 12) }}</span>
          <span class="countdown" :class="{ due: countdown(item.scheduledEnqueueTimeUtc) === 'due' }">
            {{ countdown(item.scheduledEnqueueTimeUtc) }}
          </span>
        </div>

        <div v-if="editing === item.message.sequenceNumber" class="edit" @click.stop>
          <input v-model="draft" type="datetime-local" step="1" @keyup.enter="saveEdit(item)" @keyup.esc="editing = null" />
          <button class="icon ok" title="Save" @click="saveEdit(item)"><Check :size="12" /></button>
          <button class="icon" title="Discard" @click="editing = null"><X :size="12" /></button>
        </div>
        <div v-else class="meta">
          <span class="when" :title="item.scheduledEnqueueTimeUtc ?? ''">
            {{ item.scheduledEnqueueTimeUtc ? new Date(item.scheduledEnqueueTimeUtc).toLocaleString() : '—' }}
          </span>
          <span class="actions" @click.stop>
            <button class="icon" title="Change delivery time" :disabled="busy" @click="startEdit(item)"><Pencil :size="11" /></button>
            <button class="icon ok" title="Deliver now" :disabled="busy" @click="deliverNow(item)"><Play :size="11" /></button>
            <button class="icon danger" title="Cancel (never deliver)" :disabled="busy" @click="cancel(item)"><X :size="11" /></button>
          </span>
        </div>

        <div v-if="!entity" class="target">→ {{ item.entityName }} · #{{ item.message.sequenceNumber }}</div>
      </div>

      <div v-if="loaded && items.length === 0" class="empty">No scheduled messages</div>
      <div v-else-if="!loaded" class="empty">Loading…</div>
    </div>
  </div>
</template>

<style scoped>
.scheduled { display: flex; flex-direction: column; flex: 1; min-height: 0; }
.toolbar { display: flex; align-items: center; justify-content: space-between; gap: 8px; padding: 6px 12px; background: var(--bg-mantle); border-bottom: 1px solid var(--border-subtle); flex-wrap: wrap; }
.shift { display: flex; align-items: center; gap: 4px; }
.label { font-size: 11px; color: var(--text-muted); margin-right: 2px; }
.amount { width: 48px; }
.amount, select { font-size: 11px; padding: 2px 4px; border: 1px solid var(--border); border-radius: 4px; background: #fff; color: var(--text); }
button { display: inline-flex; align-items: center; gap: 4px; }
button:disabled { opacity: 0.45; cursor: default; }
.ghost { background: var(--bg-surface); color: var(--text); padding: 2px 8px; font-weight: 700; }
.primary { background: var(--blue); color: #fff; font-weight: 600; }
.error { padding: 6px 12px; font-size: 11px; color: var(--red); background: var(--bg-crust); }
.rows { flex: 1; overflow-y: auto; background: var(--bg-mantle); }
.row { padding: 8px 12px; border-bottom: 1px solid var(--border-subtle); cursor: pointer; transition: background 0.1s; }
.row:hover { background: var(--bg-surface); }
.row.selected { background: var(--bg-surface); border-left: 3px solid var(--blue); }
.header, .meta { display: flex; justify-content: space-between; align-items: center; gap: 6px; }
.msg-id { color: var(--text); font-weight: 500; font-size: 11px; font-family: 'Cascadia Code', 'Fira Code', monospace; }
.countdown { font-size: 10px; font-weight: 600; color: var(--mauve); }
.countdown.due { color: var(--green); }
.meta { margin-top: 3px; }
.when { font-size: 10px; color: var(--text-muted); }
.actions { display: flex; gap: 2px; opacity: 0.4; transition: opacity 0.1s; }
.row:hover .actions, .row.selected .actions { opacity: 1; }
.icon { background: transparent; color: var(--text-muted); padding: 2px 4px; }
.icon:hover:not(:disabled) { background: var(--bg-crust); color: var(--text); }
.icon.ok:hover:not(:disabled) { color: var(--green); }
.icon.danger:hover:not(:disabled) { color: var(--red); }
.edit { display: flex; align-items: center; gap: 4px; margin-top: 4px; }
.edit input { flex: 1; font-size: 11px; padding: 2px 4px; border: 1px solid var(--blue); border-radius: 4px; }
.target { margin-top: 3px; font-size: 10px; color: var(--blue); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.empty { padding: 20px; text-align: center; color: var(--text-muted); }
</style>
