<script setup lang="ts">
import { ref, inject, watch, onUnmounted, computed } from 'vue'
import type { MessageInfo, SubscriptionInfo, EntityType } from '../types'
import MessageRow from './MessageRow.vue'
import { useMessages } from '../composables/useMessages'
import { sseKey } from '../composables/useNamespaceSse'
import { api } from '../api/client'

const props = defineProps<{
  namespace: string
  entity: string
  entityType: EntityType | null
}>()

const emit = defineEmits<{ selectQueue: [name: string] }>()
const selectedMessage = defineModel<MessageInfo | null>('selectedMessage')

const sse = inject(sseKey)!

const {
  messages, deadLetterMessages, deadLetterViewActive, properties, connected,
  refresh, refreshDeadLetter, refreshProperties, startListening, stopListening,
} = useMessages(() => props.namespace, () => props.entity, () => props.entityType, sse)

const activeTab = ref<'messages' | 'deadletter' | 'properties'>('messages')

/** ISO 8601 duration → something a human reads at a glance ("PT5M" → "5m", "P14D" → "14d"). */
function duration(iso: string | null): string {
  if (iso === null) return 'Never (unbounded)'
  const m = /^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:([\d.]+)S)?)?$/.exec(iso)
  if (!m) return iso
  const parts: string[] = []
  if (m[1]) parts.push(`${m[1]}d`)
  if (m[2]) parts.push(`${m[2]}h`)
  if (m[3]) parts.push(`${m[3]}m`)
  if (m[4]) parts.push(`${m[4]}s`)
  return parts.length ? `${parts.join(' ')}  (${iso})` : iso
}

const propertyRows = computed<{ label: string; value: string }[]>(() => {
  const p = properties.value
  if (!p) return []
  const yesNo = (b: boolean) => (b ? 'Yes' : 'No')
  return [
    { label: 'Lock duration', value: duration(p.lockDuration) },
    { label: 'Max delivery count', value: String(p.maxDeliveryCount) },
    { label: 'Requires session', value: yesNo(p.requiresSession) },
    { label: 'Default message TTL', value: duration(p.defaultMessageTimeToLive) },
    { label: 'Dead-letter on expiration', value: yesNo(p.deadLetteringOnMessageExpiration) },
    { label: 'Duplicate detection', value: p.requiresDuplicateDetection ? `Yes, window ${duration(p.duplicateDetectionHistoryTimeWindow)}` : 'No' },
    { label: 'Batched operations', value: yesNo(p.enableBatchedOperations) },
    { label: 'Partitioning', value: yesNo(p.enablePartitioning) },
    { label: 'Express', value: yesNo(p.enableExpress) },
    { label: 'Max size', value: `${p.maxSizeInMegabytes} MB` },
    { label: 'Auto-delete on idle', value: p.autoDeleteOnIdle ? duration(p.autoDeleteOnIdle) : 'Never' },
    { label: 'Forward to', value: p.forwardTo ?? '—' },
    { label: 'Forward dead-letters to', value: p.forwardDeadLetteredMessagesTo ?? '—' },
    { label: 'User metadata', value: p.userMetadata ?? '—' },
    { label: 'Active messages', value: String(p.messageCount) },
    { label: 'Dead-lettered', value: String(p.deadLetterCount) },
    { label: 'Total received', value: String(p.totalMessageCount) },
    { label: 'Completed', value: String(p.consumedCount) },
    ...(p.requiresSession ? [{ label: 'Sessions seen', value: String(p.sessionCount) }] : []),
  ]
})
const hideConsumed = ref(false)
const visibleMessages = computed(() =>
  hideConsumed.value ? messages.value.filter(m => m.state !== 'Consumed' && m.state !== 'DeadLettered') : messages.value
)

// When viewing a topic, fetch its subscriptions
const topicSubscriptions = ref<SubscriptionInfo[]>([])

async function refreshSubscriptions() {
  if (props.entityType !== 'topic') { topicSubscriptions.value = []; return }
  try {
    const data = await api.getEntities(props.namespace)
    const topic = data.topics.find(t => t.name === props.entity)
    topicSubscriptions.value = topic?.subscriptions ?? []
  } catch { topicSubscriptions.value = [] }
}

function switchTab(tab: 'messages' | 'deadletter' | 'properties') {
  activeTab.value = tab
  selectedMessage.value = null
  deadLetterViewActive.value = tab === 'deadletter'
  if (tab === 'deadletter') refreshDeadLetter()
  if (tab === 'properties') refreshProperties()
}

watch(() => [props.namespace, props.entity, props.entityType], () => {
  selectedMessage.value = null
  activeTab.value = 'messages'
  deadLetterViewActive.value = false
  messages.value = []
  deadLetterMessages.value = []
  properties.value = null
  if (props.entityType === 'queue' || props.entityType === 'subscription') {
    refresh()
    startListening()
    topicSubscriptions.value = []
  } else if (props.entityType === 'topic') {
    stopListening()
    refreshSubscriptions()
  }
}, { immediate: true })

onUnmounted(stopListening)

function shortName(name: string) {
  const idx = name.lastIndexOf('/')
  return idx > 0 ? name.substring(idx + 1) : name
}

function parentPath(name: string) {
  const idx = name.lastIndexOf('/')
  return idx > 0 ? name.substring(0, idx) : null
}
</script>

<template>
  <div class="message-list">
    <div class="entity-header">
      <div class="title-row">
        <span class="name">{{ shortName(entity) }}</span>
        <span class="type-badge"><EntityIcon :type="entityType" :size="11" />{{ entityType }}</span>
      </div>
      <div v-if="parentPath(entity)" class="parent">{{ parentPath(entity) }}</div>
    </div>

    <!-- Queue / subscription view: messages -->
    <template v-if="entityType === 'queue' || entityType === 'subscription'">
      <div class="tabs">
        <div class="tab" :class="{ active: activeTab === 'messages' }" @click="switchTab('messages')"><Mail :size="12" />Messages</div>
        <div class="tab" :class="{ active: activeTab === 'deadletter' }" @click="switchTab('deadletter')"><MailWarning :size="12" />Dead Letter</div>
        <div v-if="entityType === 'queue'" class="tab" :class="{ active: activeTab === 'properties' }" @click="switchTab('properties')"><Settings2 :size="12" />Properties</div>
      </div>

      <!-- Messages tab -->
      <template v-if="activeTab === 'messages'">
        <div class="list-toolbar">
          <span class="live-indicator" :class="{ connected }">
            {{ connected ? '\u25CF Live' : '\u25CB Disconnected' }}
          </span>
          <label class="toggle">
            <input type="checkbox" v-model="hideConsumed" />
            <span>Hide consumed</span>
          </label>
        </div>

        <div class="rows">
          <MessageRow
            v-for="msg in visibleMessages" :key="msg.messageId"
            :message="msg"
            :selected="selectedMessage?.messageId === msg.messageId"
            @select="selectedMessage = msg"
          />
          <div v-if="visibleMessages.length === 0" class="empty">
            No messages
          </div>
        </div>
      </template>

      <!-- Dead Letter tab -->
      <template v-if="activeTab === 'deadletter'">
        <div class="rows">
          <MessageRow
            v-for="msg in deadLetterMessages" :key="msg.messageId"
            :message="msg"
            :selected="selectedMessage?.messageId === msg.messageId"
            @select="selectedMessage = msg"
          />
          <div v-if="deadLetterMessages.length === 0" class="empty">
            No dead-letter messages
          </div>
        </div>
      </template>

      <!-- Properties tab -->
      <template v-if="activeTab === 'properties'">
        <div class="rows">
          <div v-if="propertyRows.length === 0" class="empty">Loading properties…</div>
          <div v-for="row in propertyRows" :key="row.label" class="prop-row">
            <span class="prop-key">{{ row.label }}</span>
            <span class="prop-val">{{ row.value }}</span>
          </div>
        </div>
      </template>
    </template>

    <!-- Topic view: subscriptions -->
    <template v-else-if="entityType === 'topic'">
      <div class="tabs">
        <div class="tab active"><CornerDownRight :size="12" />Subscriptions</div>
      </div>

      <div class="rows">
        <div
          v-for="sub in topicSubscriptions" :key="sub.name"
          class="sub-item"
          :class="{ clickable: !!sub.forwardTo }"
          @click="sub.forwardTo && emit('selectQueue', sub.forwardTo)"
        >
          <div class="sub-header">
            <span class="sub-name">{{ sub.name }}</span>
            <span v-if="sub.messageCount > 0" class="badge">{{ sub.messageCount }}</span>
          </div>
          <div v-if="sub.forwardTo" class="sub-forward">
            → {{ sub.forwardTo }}
          </div>
          <div class="sub-meta">
            {{ sub.ruleCount }} rule{{ sub.ruleCount !== 1 ? 's' : '' }}
          </div>
        </div>
        <div v-if="topicSubscriptions.length === 0" class="empty">
          No subscriptions
        </div>
      </div>
    </template>
  </div>
</template>

<style scoped>
.message-list { width: 380px; border-right: 1px solid var(--border); display: flex; flex-direction: column; flex-shrink: 0; }
.entity-header { padding: 12px 14px; border-bottom: 1px solid var(--dark-border); background: var(--dark); }
.title-row { display: flex; align-items: center; gap: 8px; }
.name { color: var(--dark-text); font-weight: 700; font-size: 14px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.type-badge { background: var(--blue); color: #fff; padding: 2px 8px; border-radius: 4px; font-size: 10px; font-weight: 600; text-transform: uppercase; flex-shrink: 0; opacity: 0.85; display: inline-flex; align-items: center; gap: 4px; }
.type-badge .entity-icon { opacity: 1; }
.parent { font-size: 10px; color: var(--dark-text-muted); margin-top: 2px; }
.tabs { display: flex; border-bottom: 1px solid var(--dark-border); background: var(--dark); }
.tab { padding: 8px 14px; color: var(--dark-text-muted); font-size: 12px; cursor: pointer; transition: color 0.1s; border-bottom: 2px solid transparent; display: inline-flex; align-items: center; gap: 5px; }
.tab:hover { color: var(--dark-text); }
.tab.active { border-bottom-color: var(--blue); color: var(--blue); font-weight: 700; }
.list-toolbar { display: flex; align-items: center; justify-content: space-between; padding: 6px 12px; background: var(--bg-mantle); border-bottom: 1px solid var(--border-subtle); }
.live-indicator { font-size: 11px; font-weight: 500; color: var(--text-muted); }
.live-indicator.connected { color: var(--green); }
.toggle { display: flex; align-items: center; gap: 5px; font-size: 11px; color: var(--text-muted); cursor: pointer; user-select: none; }
.toggle input { accent-color: var(--blue); width: 12px; height: 12px; }
.rows { flex: 1; overflow-y: auto; background: var(--bg-mantle); }
.rows::-webkit-scrollbar { width: 6px; }
.rows::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
.rows::-webkit-scrollbar-track { background: transparent; }
.empty { padding: 20px; text-align: center; color: var(--text-muted); }
.prop-row { display: grid; grid-template-columns: 180px 1fr; gap: 4px 12px; padding: 7px 12px; border-bottom: 1px solid var(--border-subtle); font-size: 11px; }
.prop-key { color: var(--text-muted); font-weight: 500; }
.prop-val { color: var(--text); font-family: 'Cascadia Code', 'Fira Code', monospace; word-break: break-all; }

.sub-item { padding: 10px 12px; border-bottom: 1px solid var(--border-subtle); transition: background 0.1s; }
.sub-item.clickable { cursor: pointer; }
.sub-item.clickable:hover { background: var(--bg-surface); }
.sub-header { display: flex; justify-content: space-between; align-items: center; }
.sub-name { color: var(--text); font-weight: 600; font-size: 13px; }
.badge { background: var(--blue); color: #fff; border-radius: 8px; padding: 1px 6px; font-size: 10px; font-weight: 600; }
.sub-forward { color: var(--blue); font-size: 10px; margin-top: 3px; }
.sub-meta { color: var(--text-muted); font-size: 9px; margin-top: 2px; }
</style>
