<script setup lang="ts">
import { inject, onMounted, onUnmounted, watch } from 'vue'
import NamespaceSelector from './NamespaceSelector.vue'
import ConnectionStringBar from './ConnectionStringBar.vue'
import { useEntities } from '../composables/useEntities'
import { sseKey } from '../composables/useNamespaceSse'
import type { EntityType } from '../types'
import { ChevronRight, ChevronDown, ArrowRight } from 'lucide-vue-next'
import EntityIcon from './EntityIcon.vue'

const ns = defineModel<string>('namespace', { required: true })
const entity = defineModel<string | null>('entity', { required: true })
const entityType = defineModel<EntityType | null>('entityType', { required: true })
const emit = defineEmits<{ select: [] }>()

const sse = inject(sseKey)!

const {
  namespaces, loading, filter, showAll, topicGroups, filteredQueues,
  totalQueues, totalTopics,
  toggleGroup, isCollapsed, start, stop, onNamespaceChange,
} = useEntities(() => ns.value, sse)

onMounted(start)
onUnmounted(stop)

watch(ns, () => {
  entity.value = null
  entityType.value = null
  onNamespaceChange()
})

function selectEntity(name: string, type: EntityType) {
  entity.value = name
  entityType.value = type
  emit('select')
}

function selectSubscription(topicName: string, subName: string) {
  selectEntity(`${topicName}/subscriptions/${subName}`, 'subscription')
}

function shortName(fullName: string) {
  const idx = fullName.lastIndexOf('/')
  return idx > 0 ? fullName.substring(idx + 1) : fullName
}
</script>

<template>
  <div class="sidebar">
    <NamespaceSelector :namespaces="namespaces" v-model:selected="ns" />

    <div class="search">
      <input v-model="filter" placeholder="Filter entities..." />
    </div>

    <div class="toggle-row">
      <label class="toggle">
        <input type="checkbox" v-model="showAll" />
        <span>Show all entities</span>
      </label>
    </div>

    <div class="tree">
      <div v-if="loading" class="loading">Loading entities...</div>
      <template v-else>

      <div class="section-header">{{ showAll ? 'All Queues' : 'Active Queues' }}</div>
      <div v-if="!showAll && filteredQueues.length === 0" class="empty-hint">
        No queues with messages
      </div>
      <div
        v-for="q in filteredQueues" :key="q.name"
        class="entity-row" :class="{ selected: entity === q.name }"
        @click="selectEntity(q.name, 'queue')"
      >
        <span class="entity-name"><EntityIcon type="queue" />{{ q.name }}</span>
        <span class="entity-badges">
          <span v-if="q.totalMessageCount - q.consumedCount - q.deadLetterCount > 0" class="badge" title="Active messages">{{ q.totalMessageCount - q.consumedCount - q.deadLetterCount }}</span>
          <span v-if="q.consumedCount > 0" class="badge-green" title="Completed">{{ q.consumedCount }}</span>
          <span v-if="q.deadLetterCount > 0" class="badge-red" title="Dead-lettered">{{ q.deadLetterCount }}</span>
        </span>
      </div>

      <div class="section-header">{{ showAll ? 'All Topics' : 'Active Topics' }}</div>
      <div v-if="!showAll && topicGroups.length === 0" class="empty-hint">
        No topics with messages
      </div>
      <template v-for="group in topicGroups" :key="group.prefix">
        <div class="group-header" @click="toggleGroup(group.prefix)">
          <component :is="isCollapsed(group.prefix) ? ChevronRight : ChevronDown" class="chevron" :size="12" />
          {{ group.prefix }}
        </div>
        <template v-if="!isCollapsed(group.prefix)">
          <template v-for="t in group.topics" :key="t.name">
            <div
              class="entity-row indent" :class="{ selected: entity === t.name }"
              @click="selectEntity(t.name, 'topic')"
            >
              <span class="entity-name"><EntityIcon type="topic" />{{ shortName(t.name) }}</span>
            </div>
            <div
              v-for="s in t.subscriptions" :key="s.name"
              class="sub-row clickable"
              :class="{ selected: s.forwardTo ? entity === s.forwardTo : entity === `${t.name}/subscriptions/${s.name}` }"
              @click="s.forwardTo ? selectEntity(s.forwardTo, 'queue') : selectSubscription(t.name, s.name)"
            >
              <EntityIcon type="subscription" :size="12" />{{ s.name }}
              <span v-if="s.forwardTo" class="forward-to"><ArrowRight :size="10" />{{ s.forwardTo }}</span>
              <span v-if="s.messageCount > 0" class="badge-sm" title="Active messages">{{ s.messageCount }}</span>
              <span v-if="s.deadLetterCount > 0" class="badge-sm badge-sm-red" title="Dead-lettered">{{ s.deadLetterCount }}</span>
            </div>
          </template>
        </template>
      </template>

      </template>
    </div>

    <ConnectionStringBar />

    <div class="footer">
      {{ filteredQueues.length }}<span v-if="!showAll">/{{ totalQueues }}</span> queues ·
      {{ topicGroups.reduce((n, g) => n + g.topics.length, 0) }}<span v-if="!showAll">/{{ totalTopics }}</span> topics
    </div>
  </div>
</template>

<style scoped>
.sidebar { width: 300px; border-right: 1px solid rgba(0,0,0,0.2); display: flex; flex-direction: column; flex-shrink: 0; background: var(--dark); color: var(--dark-text); }
.search { padding: 10px 10px 6px; }
.search input { width: 100%; background: var(--dark-surface); border: 1px solid var(--dark-border); border-radius: 6px; padding: 7px 10px; color: var(--dark-text); font-size: 12px; outline: none; transition: border-color 0.15s; }
.search input:focus { border-color: var(--blue); }
.search input::placeholder { color: var(--dark-text-muted); }
.toggle-row { padding: 0 10px 6px; }
.toggle { display: flex; align-items: center; gap: 6px; font-size: 11px; color: var(--dark-text-muted); cursor: pointer; user-select: none; }
.toggle input { accent-color: var(--blue); width: 12px; height: 12px; }
.tree { flex: 1; overflow-y: auto; padding: 4px 8px; }
.tree::-webkit-scrollbar { width: 6px; }
.tree::-webkit-scrollbar-thumb { background: var(--dark-surface); border-radius: 3px; }
.tree::-webkit-scrollbar-track { background: transparent; }
.section-header { color: var(--dark-text-muted); font-size: 10px; text-transform: uppercase; padding: 10px 4px 4px; font-weight: 700; letter-spacing: 0.8px; }
.empty-hint { padding: 4px 12px; font-size: 10px; color: var(--dark-text-muted); font-style: italic; }
.group-header { padding: 4px 4px 4px 12px; color: var(--dark-text-muted); cursor: pointer; font-size: 12px; font-weight: 600; user-select: none; transition: color 0.1s; }
.group-header:hover { color: var(--dark-text); }
.chevron { color: var(--dark-text-muted); margin-right: 4px; vertical-align: -2px; }
.entity-row { padding: 5px 6px 5px 12px; cursor: pointer; border-radius: 5px; font-size: 12px; font-weight: 500; display: flex; justify-content: space-between; align-items: center; color: var(--dark-text); transition: background 0.1s; }
.entity-row.indent { padding-left: 28px; }
.entity-row:hover { background: var(--dark-surface); }
.entity-row.selected { background: var(--blue); color: #fff; }
.entity-row.selected .badge { background: rgba(255,255,255,0.25); }
.entity-row.selected .badge-green { background: rgba(255,255,255,0.2); }
.entity-row.selected .badge-red { background: rgba(255,255,255,0.2); }
.entity-name { display: flex; align-items: center; gap: 6px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; min-width: 0; }
.entity-row .entity-icon { color: var(--dark-text-muted); }
.entity-row.selected .entity-icon { color: #fff; opacity: 1; }
.entity-badges { display: flex; gap: 3px; flex-shrink: 0; }
.badge { background: var(--blue); color: #fff; border-radius: 8px; padding: 1px 6px; font-size: 10px; font-weight: 600; }
.badge-green { background: var(--green); color: #fff; border-radius: 8px; padding: 1px 6px; font-size: 10px; font-weight: 600; }
.badge-red { background: var(--red); color: #fff; border-radius: 8px; padding: 1px 6px; font-size: 10px; font-weight: 600; }
.badge-sm { background: var(--dark-surface); color: var(--dark-text-muted); border-radius: 8px; padding: 0 4px; font-size: 9px; margin-left: 4px; }
.badge-sm-red { background: var(--red); color: #fff; }
.sub-row { padding: 3px 4px 3px 40px; color: var(--dark-text-muted); font-size: 11px; display: flex; align-items: center; gap: 5px; }
.sub-row.clickable { cursor: pointer; border-radius: 4px; transition: background 0.1s; }
.sub-row.clickable:hover { background: var(--dark-surface); color: var(--dark-text); }
.sub-row.selected { color: var(--blue); }
.forward-to { color: var(--dark-text-muted); font-size: 9px; margin-left: 2px; display: inline-flex; align-items: center; gap: 2px; }
.loading { padding: 20px; text-align: center; color: var(--dark-text-muted); font-size: 11px; animation: pulse 1.5s ease-in-out infinite; }
@keyframes pulse { 0%, 100% { opacity: 0.4; } 50% { opacity: 1; } }
.footer { border-top: 1px solid var(--dark-border); padding: 8px 12px; background: var(--dark); font-size: 10px; color: var(--dark-text-muted); }
</style>
