<script setup lang="ts">
import type { MessageInfo } from '../types'

defineProps<{ message: MessageInfo; selected: boolean }>()
defineEmits<{ select: [] }>()

function timeAgo(iso: string): string {
  const diff = Date.now() - new Date(iso).getTime()
  if (diff < 60000) return `${Math.floor(diff / 1000)}s ago`
  if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`
  return `${Math.floor(diff / 3600000)}h ago`
}
</script>

<template>
  <div class="row" :class="{ selected, consumed: message.state === 'Consumed', deadlettered: message.state === 'DeadLettered' }" @click="$emit('select')">
    <div class="header">
      <span class="msg-id">
        <span v-if="message.state === 'Consumed'" class="state-icon consumed-icon" title="Consumed">✓</span>
        <span v-else-if="message.state === 'DeadLettered'" class="state-icon dl-icon" title="Dead-lettered">✗</span>
        {{ message.messageId.substring(0, 12) }}
      </span>
      <span class="time">{{ timeAgo(message.enqueuedTimeUtc) }}</span>
    </div>
    <div v-if="message.deadLetterReason" class="dl-reason" :title="message.deadLetterErrorDescription ?? ''">
      {{ message.deadLetterReason }}
    </div>
    <div v-if="message.scalarProperties" class="tags">
      <span
        v-for="(val, key) in message.scalarProperties" :key="key"
        class="tag"
      >
        {{ key }}: {{ JSON.stringify(val) }}
      </span>
    </div>
  </div>
</template>

<style scoped>
.row { padding: 8px 12px; border-bottom: 1px solid var(--border-subtle); cursor: pointer; transition: background 0.1s; }
.row:hover { background: var(--bg-surface); }
.row.selected { background: var(--bg-surface); border-left: 3px solid var(--blue); }
.row.consumed { opacity: 0.55; }
.row.deadlettered { opacity: 0.55; }
.header { display: flex; justify-content: space-between; align-items: center; }
.msg-id { color: var(--text); font-weight: 500; font-size: 11px; display: flex; align-items: center; gap: 4px; font-family: 'Cascadia Code', 'Fira Code', monospace; }
.state-icon { font-size: 12px; font-weight: 700; }
.consumed-icon { color: var(--green); }
.dl-icon { color: var(--red); }
.time { color: var(--text-muted); font-size: 9px; }
.dl-reason { margin-top: 4px; color: var(--red); font-size: 10px; font-family: 'Cascadia Code', 'Fira Code', monospace; }
.tags { display: flex; gap: 4px; margin-top: 5px; flex-wrap: wrap; }
.tag { background: var(--bg-crust); color: var(--green); padding: 1px 6px; border-radius: 4px; font-size: 9px; font-family: 'Cascadia Code', 'Fira Code', monospace; }
.tag:nth-child(even) { color: var(--yellow); }
</style>
