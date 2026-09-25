<script setup lang="ts">
import { shallowRef, watch, provide, onUnmounted, onMounted } from 'vue'
import type { MessageInfo, EntityType } from './types'
import EntityTree from './components/EntityTree.vue'
import MessageList from './components/MessageList.vue'
import MessageDetail from './components/MessageDetail.vue'
import ScheduledMessages from './components/ScheduledMessages.vue'
import { useNamespaceSse, sseKey } from './composables/useNamespaceSse'

function readHash(): { ns: string; entity?: string; type?: EntityType; scheduled?: boolean } {
  const hash = location.hash.replace(/^#\/?/, '')
  if (!hash) return { ns: 'default' }
  const parts = hash.split('/')
  // Format: #namespace or #namespace/queue/entityName or #namespace/topic/entityName
  // (a subscription's entityName is itself "topicName/subscriptions/subName"),
  // or #namespace/scheduled for the namespace-wide scheduled-messages view
  const ns = decodeURIComponent(parts[0]) || 'default'
  if (parts.length === 2 && parts[1] === 'scheduled') return { ns, scheduled: true }
  if (parts.length >= 3) {
    const type = parts[1] as EntityType
    const entity = decodeURIComponent(parts.slice(2).join('/'))
    return { ns, entity, type }
  }
  return { ns }
}

const initial = readHash()
const selectedNamespace = shallowRef(initial.ns)
const selectedEntity = shallowRef<string | null>(initial.entity ?? null)
const selectedEntityType = shallowRef<EntityType | null>(initial.type ?? null)
const selectedMessage = shallowRef<MessageInfo | null>(null)
const scheduledView = shallowRef(initial.scheduled ?? false)

// Sync state → URL hash
function updateHash() {
  const ns = encodeURIComponent(selectedNamespace.value)
  if (selectedEntity.value && selectedEntityType.value) {
    const entity = encodeURIComponent(selectedEntity.value)
    location.hash = `${ns}/${selectedEntityType.value}/${entity}`
  } else if (scheduledView.value) {
    location.hash = `${ns}/scheduled`
  } else {
    location.hash = ns
  }
}

watch([selectedNamespace, selectedEntity, selectedEntityType, scheduledView], updateHash)

// Sync URL hash → state (browser back/forward)
function onHashChange() {
  const { ns, entity, type, scheduled } = readHash()
  if (ns !== selectedNamespace.value) selectedNamespace.value = ns
  selectedEntity.value = entity ?? null
  selectedEntityType.value = type ?? null
  scheduledView.value = scheduled ?? false
  selectedMessage.value = null
}

onMounted(() => window.addEventListener('hashchange', onHashChange))
onUnmounted(() => window.removeEventListener('hashchange', onHashChange))

const sse = useNamespaceSse()
provide(sseKey, sse)

// Connect SSE when namespace changes
watch(selectedNamespace, (ns) => {
  sse.connect(ns)
}, { immediate: true })

onUnmounted(() => sse.disconnect())
</script>

<template>
  <div class="app">
    <EntityTree
      v-model:namespace="selectedNamespace"
      v-model:entity="selectedEntity"
      v-model:entityType="selectedEntityType"
      v-model:scheduledView="scheduledView"
      @select="selectedMessage = null"
    />
    <div v-if="scheduledView && !selectedEntity" class="scheduled-panel">
      <div class="panel-header">
        <span class="panel-title">Scheduled messages</span>
        <span class="panel-ns">namespace: {{ selectedNamespace }}</span>
      </div>
      <ScheduledMessages :namespace="selectedNamespace" v-model:selectedMessage="selectedMessage" />
    </div>
    <MessageList
      v-if="selectedEntity"
      :namespace="selectedNamespace"
      :entity="selectedEntity"
      :entity-type="selectedEntityType"
      v-model:selectedMessage="selectedMessage"
      @selectQueue="(q) => { selectedEntity = q; selectedEntityType = 'queue'; selectedMessage = null }"
    />
    <MessageDetail
      v-if="selectedMessage"
      :message="selectedMessage"
    />
    <div v-if="!selectedEntity && !scheduledView" class="empty-state">
      <p>Select an entity from the sidebar to browse messages</p>
    </div>
  </div>
</template>

<style scoped>
.app {
  display: flex;
  height: 100vh;
  overflow: hidden;
}
.scheduled-panel { width: 460px; border-right: 1px solid var(--border); display: flex; flex-direction: column; flex-shrink: 0; }
.panel-header { padding: 12px 14px; border-bottom: 1px solid var(--dark-border); background: var(--dark); display: flex; flex-direction: column; gap: 2px; }
.panel-title { color: var(--dark-text); font-weight: 700; font-size: 14px; }
.panel-ns { font-size: 10px; color: var(--dark-text-muted); }
.empty-state {
  flex: 1;
  display: flex;
  align-items: center;
  justify-content: center;
  color: var(--text-muted);
}
</style>
