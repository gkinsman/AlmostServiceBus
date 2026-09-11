<script setup lang="ts">
import { shallowRef, watch, provide, onUnmounted, onMounted } from 'vue'
import type { MessageInfo, EntityType } from './types'
import EntityTree from './components/EntityTree.vue'
import MessageList from './components/MessageList.vue'
import MessageDetail from './components/MessageDetail.vue'
import { useNamespaceSse, sseKey } from './composables/useNamespaceSse'

function readHash(): { ns: string; entity?: string; type?: EntityType } {
  const hash = location.hash.replace(/^#\/?/, '')
  if (!hash) return { ns: 'default' }
  const parts = hash.split('/')
  // Format: #namespace or #namespace/queue/entityName or #namespace/topic/entityName
  // (a subscription's entityName is itself "topicName/subscriptions/subName")
  const ns = decodeURIComponent(parts[0]) || 'default'
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

// Sync state → URL hash
function updateHash() {
  const ns = encodeURIComponent(selectedNamespace.value)
  if (selectedEntity.value && selectedEntityType.value) {
    const entity = encodeURIComponent(selectedEntity.value)
    location.hash = `${ns}/${selectedEntityType.value}/${entity}`
  } else {
    location.hash = ns
  }
}

watch([selectedNamespace, selectedEntity, selectedEntityType], updateHash)

// Sync URL hash → state (browser back/forward)
function onHashChange() {
  const { ns, entity, type } = readHash()
  if (ns !== selectedNamespace.value) selectedNamespace.value = ns
  selectedEntity.value = entity ?? null
  selectedEntityType.value = type ?? null
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
      @select="selectedMessage = null"
    />
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
    <div v-if="!selectedEntity" class="empty-state">
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
.empty-state {
  flex: 1;
  display: flex;
  align-items: center;
  justify-content: center;
  color: var(--text-muted);
}
</style>
